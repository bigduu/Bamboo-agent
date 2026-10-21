use super::*;
use crate::Config;

/// Redacted comparison with one supplied target snapshot. This is not an
/// authorization token; the apply boundary must validate again and use CAS.
pub fn preview_bundle(bundle: &ShareBundle, target: &Config) -> ShareResult<SharePreview> {
    bundle.validate()?;
    let instances = selection::configured_instances(target);
    let scenarios = selection::scenario_refs(target);
    Ok(SharePreview {
        providers: bundle
            .providers
            .iter()
            .map(|p| ShareProviderPreview {
                id: p.id.clone(),
                provider_type: p.provider_type,
                label: p.label.clone(),
                has_api_key: p.api_key.is_some(),
                has_base_url: p.base_url.is_some(),
                conflict: instances.contains_key(&p.id),
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
                conflict: target.env_vars.iter().any(|e| e.name == entry.name),
            })
            .collect(),
        contains_secrets: bundle.contains_secrets(),
    })
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
    let source_ids: BTreeSet<_> = bundle.providers.iter().map(|p| p.id.as_str()).collect();
    let env_ids: BTreeSet<_> = bundle.env.iter().map(|p| p.name.as_str()).collect();
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
        || decisions
            .env
            .keys()
            .any(|id| !env_ids.contains(id.as_str()))
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
        let exists = |name: &str| target.env_vars.iter().any(|e| e.name == name);
        let name = choose_target(
            &entry.name,
            exists(&entry.name),
            decisions.env.get(&entry.name),
        )?;
        let Some(name) = name else {
            continue;
        };
        validate_env_name(&name)?;
        if name != entry.name && (exists(&name) || env_ids.contains(name.as_str())) {
            return Err(ShareError::Conflict);
        }
        if !assigned_env.insert(name.clone()) {
            return Err(ShareError::Conflict);
        }
        let mut entry = entry.clone();
        entry.name = name;
        plan.env.push(entry);
    }
    plan.validate()?;
    Ok(plan)
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
