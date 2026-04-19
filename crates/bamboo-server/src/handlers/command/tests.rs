use bamboo_engine::SkillDefinition;

use super::sources::skill_to_command;

#[test]
fn skill_to_command_maps_core_fields() {
    let skill =
        SkillDefinition::new("sample", "Sample", "Demo skill", "Use me").with_tool_ref("read_file");
    let command = skill_to_command(&skill);

    assert_eq!(command.id, "skill-sample");
    assert_eq!(command.name, "sample");
    assert_eq!(command.display_name, "Sample");
    assert_eq!(command.command_type, "skill");
}
