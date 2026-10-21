use super::*;
use crate::Config;

/// Redacted comparison with one supplied target snapshot. This is not an
/// authorization token; the apply boundary must validate again and use CAS.
pub fn preview_bundle(bundle: &ShareBundle, target: &Config) -> ShareResult<SharePreview> {
    bundle.validate()?;
    let env_names = target_env_names(bundle, target)?;
    let instances = selection::configured_instances(target);
    let scenarios = selection::scenario_refs(target);
    Ok(SharePreview {
        providers: bundle
            .providers
            .iter()
            .map(|p| {
                let existing = instances.get(&p.id);
                let source_origin = safe_origin(p.base_url.as_deref());
                let target_origin = safe_origin(existing.and_then(|p| p.base_url.as_deref()));
                let retains_existing_api_key = existing.is_some_and(|target| {
                    crate::provider_instance_api_key_from_env(target)
                        || (p.api_key.is_none()
                            && (target.credential_ref.is_some()
                                || target.api_key_encrypted.is_some()
                                || !target.api_key.is_empty()))
                });
                let credential_origin_risk =
                    existing.is_some() && p.base_url.is_some() && source_origin != target_origin;
                ShareProviderPreview {
                    id: p.id.clone(),
                    provider_type: p.provider_type,
                    label: p.label.clone(),
                    has_api_key: p.api_key.is_some(),
                    has_base_url: p.base_url.is_some(),
                    source_origin,
                    target_origin,
                    retains_existing_api_key,
                    credential_origin_risk,
                    conflict: existing.is_some(),
                    replace_allowed: existing
                        .is_none_or(|target| target.provider_type == p.provider_type.as_str()),
                }
            })
            .collect(),
        scenarios: bundle
            .scenarios
            .iter()
            .map(|(scenario, r)| ShareScenarioPreview {
                scenario: *scenario,
                provider: r.provider.clone(),
                model: r.model.clone(),
                conflict: scenarios.contains_key(scenario),
            })
            .collect(),
        subagent_models: bundle
            .subagent_models
            .iter()
            .map(|(name, r)| ShareSubagentPreview {
                name: name.clone(),
                provider: r.provider.clone(),
                model: r.model.clone(),
                conflict: target
                    .defaults
                    .as_ref()
                    .is_some_and(|d| d.subagent_models.contains_key(name)),
            })
            .collect(),
        env: bundle
            .env
            .iter()
            .map(|entry| ShareEnvPreview {
                name: entry.name.clone(),
                has_value: entry.value.is_some(),
                secret: entry.secret,
                conflict: env_names.contains_key(&canonical_env_name(&entry.name)),
            })
            .collect(),
        contains_secrets: bundle.contains_secrets(),
    })
}

fn safe_origin(value: Option<&str>) -> Option<String> {
    let url = url::Url::parse(value?).ok()?;
    (matches!(url.scheme(), "http" | "https") && url.host_str().is_some())
        .then(|| url.origin().ascii_serialization())
}

/// Resolve only imported data; return a write plan with omission semantics
/// intact. It does not merge into Config or mutate any host state. Conflicts
/// must receive explicit decisions. Rename never overwrites another identity.
pub fn plan_import(
    bundle: &ShareBundle,
    target: &Config,
    decisions: &ShareDecisions,
) -> ShareResult<ShareBundle> {
    bundle.validate()?;
    let env_names = target_env_names(bundle, target)?;
    let source_ids: BTreeSet<_> = bundle.providers.iter().map(|p| p.id.as_str()).collect();
    let env_ids: BTreeSet<_> = bundle
        .env
        .iter()
        .map(|p| canonical_env_name(&p.name))
        .collect();
    let mut env_decisions = BTreeMap::new();
    for (name, choice) in &decisions.env {
        let identity = canonical_env_name(name);
        if !env_ids.contains(&identity) || env_decisions.insert(identity, choice).is_some() {
            return Err(ShareError::Conflict);
        }
    }
    if decisions
        .providers
        .keys()
        .any(|id| !source_ids.contains(id.as_str()))
        || decisions
            .scenarios
            .keys()
            .any(|scenario| !bundle.scenarios.contains_key(scenario))
        || decisions
            .subagent_models
            .keys()
            .any(|name| !bundle.subagent_models.contains_key(name))
    {
        return Err(ShareError::Conflict);
    }

    let existing = selection::configured_instances(target);
    let mut remap = BTreeMap::new();
    let mut assigned = BTreeSet::new();
    let mut plan = ShareBundle::default();
    for provider in &bundle.providers {
        let id = choose_target(
            &provider.id,
            existing.contains_key(&provider.id),
            decisions.providers.get(&provider.id),
        )?;
        let Some(id) = id else {
            continue;
        };
        if id == provider.id
            && existing
                .get(&id)
                .is_some_and(|target| target.provider_type != provider.provider_type.as_str())
        {
            return Err(ShareError::Invalid);
        }
        validate_id(&id)?;
        if id != provider.id && (existing.contains_key(&id) || source_ids.contains(id.as_str())) {
            return Err(ShareError::Conflict);
        }
        if !assigned.insert(id.clone()) {
            return Err(ShareError::Conflict);
        }
        remap.insert(provider.id.clone(), id.clone());
        let mut provider = provider.clone();
        provider.id = id;
        plan.providers.push(provider);
    }
    let existing_scenarios = selection::scenario_refs(target);
    for (scenario, reference) in &bundle.scenarios {
        // Skipping an imported provider drops all dependent imported mappings;
        // it cannot silently retarget those mappings to an existing credential.
        let Some(reference) = remap_ref(reference, &remap) else {
            continue;
        };
        match decisions.scenarios.get(scenario) {
            Some(ShareConflictChoice::Skip) => continue,
            Some(ShareConflictChoice::Rename { .. }) => return Err(ShareError::Conflict),
            None if existing_scenarios.contains_key(scenario) => return Err(ShareError::Conflict),
            _ => {}
        }
        plan.scenarios.insert(*scenario, reference);
    }
    let mut assigned_subagents = BTreeSet::new();
    for (name, reference) in &bundle.subagent_models {
        let Some(reference) = remap_ref(reference, &remap) else {
            continue;
        };
        let exists = |name: &str| {
            target
                .defaults
                .as_ref()
                .is_some_and(|d| d.subagent_models.contains_key(name))
        };
        let name_out = choose_target(name, exists(name), decisions.subagent_models.get(name))?;
        let Some(name_out) = name_out else {
            continue;
        };
        validate_id(&name_out)?;
        if name_out != *name
            && (exists(&name_out) || bundle.subagent_models.contains_key(&name_out))
        {
            return Err(ShareError::Conflict);
        }
        if !assigned_subagents.insert(name_out.clone()) {
            return Err(ShareError::Conflict);
        }
        plan.subagent_models.insert(name_out, reference);
    }
    let mut assigned_env = BTreeSet::new();
    for entry in &bundle.env {
        let source_identity = canonical_env_name(&entry.name);
        let target_name = env_names.get(&source_identity);
        let choice = env_decisions.get(&source_identity).copied();
        let name = choose_target(&entry.name, target_name.is_some(), choice)?;
        let Some(mut name) = name else {
            continue;
        };
        validate_env_name(&name)?;
        let identity = canonical_env_name(&name);
        if matches!(choice, Some(ShareConflictChoice::Rename { .. })) {
            if identity == source_identity
                || env_names.contains_key(&identity)
                || env_ids.contains(&identity)
            {
                return Err(ShareError::Conflict);
            }
        } else if let Some(target_name) = target_name {
            // B2 merges by exact stored spelling. A case-only Replace must
            // update that entry rather than append a second portable identity.
            name = (*target_name).to_string();
        }
        if !assigned_env.insert(identity) {
            return Err(ShareError::Conflict);
        }
        let mut entry = entry.clone();
        entry.name = name;
        plan.env.push(entry);
    }
    plan.validate()?;
    Ok(plan)
}

fn target_env_names<'a>(
    bundle: &ShareBundle,
    target: &'a Config,
) -> ShareResult<BTreeMap<String, &'a str>> {
    let mut names = BTreeMap::new();
    // Provider/model imports leave existing env entries untouched. Portable
    // name conflicts matter only when this bundle imports environment entries.
    if bundle.env.is_empty() {
        return Ok(names);
    }
    for entry in &target.env_vars {
        if names
            .insert(canonical_env_name(&entry.name), entry.name.as_str())
            .is_some()
        {
            return Err(ShareError::Conflict);
        }
    }
    Ok(names)
}

fn remap_ref(reference: &ShareModelRef, remap: &BTreeMap<String, String>) -> Option<ShareModelRef> {
    remap
        .get(&reference.provider)
        .map(|provider| ShareModelRef {
            provider: provider.clone(),
            model: reference.model.clone(),
            reasoning_effort: reference.reasoning_effort,
        })
}

fn choose_target(
    id: &str,
    conflict: bool,
    choice: Option<&ShareConflictChoice>,
) -> ShareResult<Option<String>> {
    match choice {
        Some(ShareConflictChoice::Skip) => Ok(None),
        Some(ShareConflictChoice::Rename { target }) if target != id => Ok(Some(target.clone())),
        Some(ShareConflictChoice::Rename { .. }) => Err(ShareError::Conflict),
        Some(ShareConflictChoice::Replace) => Ok(Some(id.to_string())),
        None if conflict => Err(ShareError::Conflict),
        None => Ok(Some(id.to_string())),
    }
}
