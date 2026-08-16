use std::collections::HashSet;
use std::fmt;
use std::path::Path;
use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use serde::{Deserialize, Serialize};

const DEFAULT_LEADER: &str = "Ctrl+\\";
const DEFAULT_LEADER_TIMEOUT_MS: u64 = 1_000;
const MIN_LEADER_TIMEOUT_MS: u64 = 200;
const MAX_LEADER_TIMEOUT_MS: u64 = 5_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ActionContext {
    Global,
    Navigation,
    Help,
    Notifications,
    ServeOffer,
    QuestionOptions,
    QuestionCustom,
    QuestionNumber,
    QuestionInspect,
    SessionDeleteConfirm,
    ScheduleDeleteConfirm,
    Chat,
    ConversationBlock,
    Sessions,
    Mcp,
    Schedules,
    ScheduleForm,
    Skills,
    Config,
    ConfigEditor,
    Permissions,
    PermissionEditor,
    PermissionRuleConfirm,
    PermissionDeleteConfirm,
    PermissionModeConfirm,
    SubagentTree,
    TaskPlan,
    SessionPickerBrowse,
    SessionPickerRename,
    SessionPickerPinning,
    ModelPicker,
    CommandPalette,
}

impl ActionContext {
    pub(crate) const ALL: [Self; 32] = [
        Self::Chat,
        Self::ConversationBlock,
        Self::Global,
        Self::Navigation,
        Self::Help,
        Self::Notifications,
        Self::ServeOffer,
        Self::QuestionOptions,
        Self::QuestionCustom,
        Self::QuestionNumber,
        Self::QuestionInspect,
        Self::SessionDeleteConfirm,
        Self::ScheduleDeleteConfirm,
        Self::Sessions,
        Self::Mcp,
        Self::Schedules,
        Self::ScheduleForm,
        Self::Skills,
        Self::Config,
        Self::ConfigEditor,
        Self::Permissions,
        Self::PermissionEditor,
        Self::PermissionRuleConfirm,
        Self::PermissionDeleteConfirm,
        Self::PermissionModeConfirm,
        Self::SubagentTree,
        Self::TaskPlan,
        Self::SessionPickerBrowse,
        Self::SessionPickerRename,
        Self::SessionPickerPinning,
        Self::ModelPicker,
        Self::CommandPalette,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Global => "Global",
            Self::Navigation => "Tabs",
            Self::Help => "Help",
            Self::Notifications => "Notifications",
            Self::ServeOffer => "Server prompt",
            Self::QuestionOptions => "Question options",
            Self::QuestionCustom => "Question answer",
            Self::QuestionNumber => "Question number",
            Self::QuestionInspect => "Question inspector",
            Self::SessionDeleteConfirm => "Session delete",
            Self::ScheduleDeleteConfirm => "Schedule delete",
            Self::Chat => "Chat",
            Self::ConversationBlock => "Conversation block",
            Self::Sessions => "Sessions",
            Self::Mcp => "MCP",
            Self::Schedules => "Schedules",
            Self::ScheduleForm => "Schedule form",
            Self::Skills => "Skills",
            Self::Config => "Config",
            Self::ConfigEditor => "Config editor",
            Self::Permissions => "Permission policy",
            Self::PermissionEditor => "Permission editor",
            Self::PermissionRuleConfirm => "Global permission rule confirmation",
            Self::PermissionDeleteConfirm => "Permission delete",
            Self::PermissionModeConfirm => "Permission mode",
            Self::SubagentTree => "Sub-agent tree",
            Self::TaskPlan => "Task and plan progress",
            Self::SessionPickerBrowse => "Session picker",
            Self::SessionPickerRename => "Session rename",
            Self::SessionPickerPinning => "Session pin",
            Self::ModelPicker => "Model picker",
            Self::CommandPalette => "Command palette",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ActionId {
    QuitOrStop,
    ShowHelp,
    ShowNotifications,
    OpenCommandPalette,
    NewSession,
    ReopenPendingQuestion,
    OpenModelPicker,
    OpenSessionPicker,
    OpenSubagentTree,
    OpenTaskPlan,
    StopRun,
    ToggleDetails,
    OpenConfigTab,
    OpenSchedulesTab,
    NextTab,
    PreviousTab,
    #[serde(rename = "switch-tab-1")]
    SwitchTab1,
    #[serde(rename = "switch-tab-2")]
    SwitchTab2,
    #[serde(rename = "switch-tab-3")]
    SwitchTab3,
    #[serde(rename = "switch-tab-4")]
    SwitchTab4,
    #[serde(rename = "switch-tab-5")]
    SwitchTab5,
    #[serde(rename = "switch-tab-6")]
    SwitchTab6,
    NavigateUp,
    NavigateDown,
    PageUp,
    PageDown,
    JumpFirst,
    JumpLast,
    Activate,
    Cancel,
    Backspace,
    Refresh,
    ClearInput,
    PreviousReasoningEffort,
    NextReasoningEffort,
    Confirm,
    Reject,
    OpenSlashPalette,
    SendMessage,
    InsertNewline,
    ScrollTranscriptUp,
    ScrollTranscriptDown,
    ScrollTranscriptTop,
    ScrollTranscriptBottom,
    FocusConversationBlocks,
    ExitConversationBlocks,
    PreviousConversationBlock,
    NextConversationBlock,
    ScrollBlockUp,
    ScrollBlockDown,
    ScrollBlockPageUp,
    ScrollBlockPageDown,
    PreviousDiffHunk,
    NextDiffHunk,
    ToggleDiffWrap,
    CopyValue,
    InspectValue,
    ToggleInspectorPane,
    CustomAnswer,
    NumberAnswer,
    #[serde(rename = "quick-answer-1")]
    QuickAnswer1,
    #[serde(rename = "quick-answer-2")]
    QuickAnswer2,
    #[serde(rename = "quick-answer-3")]
    QuickAnswer3,
    #[serde(rename = "quick-answer-4")]
    QuickAnswer4,
    #[serde(rename = "quick-answer-5")]
    QuickAnswer5,
    #[serde(rename = "quick-answer-6")]
    QuickAnswer6,
    #[serde(rename = "quick-answer-7")]
    QuickAnswer7,
    #[serde(rename = "quick-answer-8")]
    QuickAnswer8,
    #[serde(rename = "quick-answer-9")]
    QuickAnswer9,
    DeleteSelection,
    NextPage,
    PreviousPage,
    ShowTools,
    NewSchedule,
    RunSchedule,
    NextField,
    PreviousField,
    EditConfig,
    SaveConfig,
    OpenPermissionPolicy,
    TogglePermissionBypass,
    NewPermissionRule,
    EditPermissionRule,
    DiagnosePermission,
    SavePermissionForm,
    RenameSession,
    ToggleSessionPin,
    LoadMore,
    ExpandTreeNode,
    CollapseTreeNode,
    OpenPendingRequest,
}

#[derive(Clone, Copy)]
struct DefaultBinding {
    context: ActionContext,
    keys: &'static str,
}

pub(crate) struct ActionSpec {
    pub(crate) id: ActionId,
    pub(crate) label: &'static str,
    pub(crate) description: &'static str,
    pub(crate) palette: bool,
    availability: ActionAvailability,
    contexts: &'static [ActionContext],
    defaults: &'static [DefaultBinding],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActionAvailability {
    Always,
    ActiveRun,
    Chat,
}

macro_rules! spec {
    ($id:ident, $label:literal, $description:literal, $palette:expr, $availability:ident,
        [$($context:ident),* $(,)?],
        [$(($default_context:ident, $keys:literal)),* $(,)?]
    ) => {
        ActionSpec {
            id: ActionId::$id,
            label: $label,
            description: $description,
            palette: $palette,
            availability: ActionAvailability::$availability,
            contexts: &[$(ActionContext::$context),*],
            defaults: &[$(DefaultBinding {
                context: ActionContext::$default_context,
                keys: $keys,
            }),*],
        }
    };
    ($id:ident, $label:literal, $description:literal, $palette:expr,
        [$($context:ident),* $(,)?],
        [$(($default_context:ident, $keys:literal)),* $(,)?]
    ) => {
        spec!(
            $id,
            $label,
            $description,
            $palette,
            Always,
            [$($context),*],
            [$(($default_context, $keys)),*]
        )
    };
}

pub(crate) static ACTION_SPECS: &[ActionSpec] = &[
    spec!(
        QuitOrStop,
        "Quit or stop",
        "Stop an active run, otherwise quit the TUI",
        false,
        [Global],
        [(Global, "Ctrl+C")]
    ),
    spec!(
        ShowHelp,
        "Show help",
        "Open the resolved contextual keybinding reference",
        true,
        [Global, Navigation],
        [(Global, "F1; Leader h"), (Navigation, "?")]
    ),
    spec!(
        ShowNotifications,
        "Show notifications",
        "Open the full status, warning, and error log",
        true,
        [Global],
        [(Global, "Ctrl+L; Leader l")]
    ),
    spec!(
        OpenCommandPalette,
        "Command palette",
        "Search built-in and session-aware commands",
        false,
        [Global],
        [(Global, "Ctrl+K; Leader k")]
    ),
    spec!(
        NewSession,
        "New session",
        "Start fresh and keep the current session in background",
        true,
        Always,
        [Global],
        [(Global, "Ctrl+N; Leader n")]
    ),
    spec!(
        ReopenPendingQuestion,
        "Reopen question",
        "Restore a dismissed pending agent question",
        false,
        [Global],
        [(Global, "Ctrl+Q; Leader q")]
    ),
    spec!(
        OpenModelPicker,
        "Select model",
        "Choose a provider-qualified model and reasoning profile",
        true,
        Chat,
        [Global],
        [(Global, "Ctrl+O; Leader m")]
    ),
    spec!(
        OpenSessionPicker,
        "Open session",
        "Search and resume an existing session",
        true,
        Chat,
        [Global],
        [(Global, "Ctrl+P; Leader p")]
    ),
    spec!(
        OpenSubagentTree,
        "Sub-agent tree",
        "Inspect and navigate the active parent/child session graph",
        true,
        Chat,
        [Global],
        [(Global, "Leader a")]
    ),
    spec!(
        OpenTaskPlan,
        "Task and plan progress",
        "Inspect the active session's live task tree and plan lifecycle",
        true,
        Chat,
        [Global],
        [(Global, "Leader t")]
    ),
    spec!(
        StopRun,
        "Stop active run",
        "Request cancellation of the active agent run",
        true,
        ActiveRun,
        [Global, Chat],
        [(Global, "Ctrl+S; Leader s"), (Chat, "Ctrl+S; Leader s")]
    ),
    spec!(
        ToggleDetails,
        "Toggle focused details",
        "Toggle the focused block or the default for new details",
        true,
        Chat,
        [Chat, ConversationBlock],
        [
            (Chat, "Ctrl+X; Leader x"),
            (ConversationBlock, "Ctrl+X; Leader x")
        ]
    ),
    spec!(
        OpenConfigTab,
        "Open config",
        "Switch to the configuration tab",
        true,
        [Global],
        []
    ),
    spec!(
        OpenSchedulesTab,
        "Open schedules",
        "Switch to the schedules tab",
        true,
        [Global],
        []
    ),
    spec!(
        NextTab,
        "Next tab",
        "Switch to the next top-level tab",
        false,
        [Global],
        [(Global, "Tab")]
    ),
    spec!(
        PreviousTab,
        "Previous tab",
        "Switch to the previous top-level tab",
        false,
        [Global],
        [(Global, "Shift+Tab")]
    ),
    spec!(
        SwitchTab1,
        "Open Chat tab",
        "Switch directly to Chat outside text entry",
        false,
        [Navigation],
        [(Navigation, "1")]
    ),
    spec!(
        SwitchTab2,
        "Open Sessions tab",
        "Switch directly to Sessions outside text entry",
        false,
        [Navigation],
        [(Navigation, "2")]
    ),
    spec!(
        SwitchTab3,
        "Open MCP tab",
        "Switch directly to MCP outside text entry",
        false,
        [Navigation],
        [(Navigation, "3")]
    ),
    spec!(
        SwitchTab4,
        "Open Schedules tab",
        "Switch directly to Schedules outside text entry",
        false,
        [Navigation],
        [(Navigation, "4")]
    ),
    spec!(
        SwitchTab5,
        "Open Skills tab",
        "Switch directly to Skills outside text entry",
        false,
        [Navigation],
        [(Navigation, "5")]
    ),
    spec!(
        SwitchTab6,
        "Open Config tab",
        "Switch directly to Config outside text entry",
        false,
        [Navigation],
        [(Navigation, "6")]
    ),
    spec!(
        NavigateUp,
        "Move up",
        "Move selection or scroll one row upward",
        false,
        [
            Help,
            Notifications,
            QuestionOptions,
            QuestionInspect,
            PermissionRuleConfirm,
            Sessions,
            Mcp,
            Schedules,
            Skills,
            Config,
            Permissions,
            SubagentTree,
            TaskPlan,
            SessionPickerBrowse,
            ModelPicker,
            CommandPalette
        ],
        [
            (Help, "Up; k"),
            (Notifications, "Up; k"),
            (QuestionOptions, "Up; k"),
            (QuestionInspect, "Up; k"),
            (PermissionRuleConfirm, "Up; k"),
            (Sessions, "Up"),
            (Mcp, "Up"),
            (Schedules, "Up"),
            (Skills, "Up"),
            (Config, "Up; k"),
            (Permissions, "Up; k"),
            (SubagentTree, "Up; k"),
            (TaskPlan, "Up; k"),
            (SessionPickerBrowse, "Up"),
            (ModelPicker, "Up"),
            (CommandPalette, "Up")
        ]
    ),
    spec!(
        NavigateDown,
        "Move down",
        "Move selection or scroll one row downward",
        false,
        [
            Help,
            Notifications,
            QuestionOptions,
            QuestionInspect,
            PermissionRuleConfirm,
            Sessions,
            Mcp,
            Schedules,
            Skills,
            Config,
            Permissions,
            SubagentTree,
            TaskPlan,
            SessionPickerBrowse,
            ModelPicker,
            CommandPalette
        ],
        [
            (Help, "Down; j"),
            (Notifications, "Down; j"),
            (QuestionOptions, "Down; j"),
            (QuestionInspect, "Down; j"),
            (PermissionRuleConfirm, "Down; j"),
            (Sessions, "Down"),
            (Mcp, "Down"),
            (Schedules, "Down"),
            (Skills, "Down"),
            (Config, "Down; j"),
            (Permissions, "Down; j"),
            (SubagentTree, "Down; j"),
            (TaskPlan, "Down; j"),
            (SessionPickerBrowse, "Down"),
            (ModelPicker, "Down"),
            (CommandPalette, "Down")
        ]
    ),
    spec!(
        PageUp,
        "Page up",
        "Move or scroll one page upward",
        false,
        [
            Help,
            Notifications,
            QuestionOptions,
            QuestionInspect,
            PermissionRuleConfirm,
            Config,
            SubagentTree,
            TaskPlan,
            ModelPicker,
            CommandPalette
        ],
        [
            (Help, "PageUp"),
            (Notifications, "PageUp"),
            (QuestionOptions, "PageUp"),
            (QuestionInspect, "PageUp"),
            (PermissionRuleConfirm, "PageUp"),
            (Config, "PageUp"),
            (SubagentTree, "PageUp"),
            (TaskPlan, "PageUp"),
            (ModelPicker, "PageUp"),
            (CommandPalette, "PageUp")
        ]
    ),
    spec!(
        PageDown,
        "Page down",
        "Move or scroll one page downward",
        false,
        [
            Help,
            Notifications,
            QuestionOptions,
            QuestionInspect,
            PermissionRuleConfirm,
            Config,
            SubagentTree,
            TaskPlan,
            ModelPicker,
            CommandPalette
        ],
        [
            (Help, "PageDown"),
            (Notifications, "PageDown"),
            (QuestionOptions, "PageDown"),
            (QuestionInspect, "PageDown"),
            (PermissionRuleConfirm, "PageDown"),
            (Config, "PageDown"),
            (SubagentTree, "PageDown"),
            (TaskPlan, "PageDown"),
            (ModelPicker, "PageDown"),
            (CommandPalette, "PageDown")
        ]
    ),
    spec!(
        JumpFirst,
        "Jump to first",
        "Move to the first item or row",
        false,
        [
            Help,
            Notifications,
            QuestionOptions,
            QuestionInspect,
            PermissionRuleConfirm,
            ConversationBlock,
            SubagentTree,
            TaskPlan,
            ModelPicker,
            CommandPalette
        ],
        [
            (Help, "Home"),
            (Notifications, "Home"),
            (QuestionOptions, "Home"),
            (QuestionInspect, "Home"),
            (PermissionRuleConfirm, "Home"),
            (ConversationBlock, "Home"),
            (SubagentTree, "Home; g g"),
            (TaskPlan, "Home; g g"),
            (ModelPicker, "Home"),
            (CommandPalette, "Home")
        ]
    ),
    spec!(
        JumpLast,
        "Jump to last",
        "Move to the final item or row",
        false,
        [
            Help,
            Notifications,
            QuestionOptions,
            QuestionInspect,
            PermissionRuleConfirm,
            ConversationBlock,
            SubagentTree,
            TaskPlan,
            ModelPicker,
            CommandPalette
        ],
        [
            (Help, "End"),
            (Notifications, "End"),
            (QuestionOptions, "End"),
            (QuestionInspect, "End"),
            (PermissionRuleConfirm, "End"),
            (ConversationBlock, "End"),
            (SubagentTree, "End; Shift+G"),
            (TaskPlan, "End; Shift+G"),
            (ModelPicker, "End"),
            (CommandPalette, "End")
        ]
    ),
    spec!(
        Activate,
        "Activate",
        "Use, submit, open, or toggle the selected item",
        false,
        [
            QuestionOptions,
            QuestionCustom,
            QuestionNumber,
            ConversationBlock,
            Notifications,
            Sessions,
            Mcp,
            ScheduleForm,
            Skills,
            Permissions,
            SubagentTree,
            SessionPickerBrowse,
            SessionPickerRename,
            ModelPicker,
            CommandPalette
        ],
        [
            (QuestionOptions, "Enter"),
            (QuestionCustom, "Enter"),
            (QuestionNumber, "Enter"),
            (ConversationBlock, "Enter"),
            (Notifications, "Enter"),
            (Sessions, "Enter"),
            (Mcp, "Enter"),
            (ScheduleForm, "Enter"),
            (Skills, "Enter"),
            (Permissions, "Enter"),
            (SubagentTree, "Enter"),
            (SessionPickerBrowse, "Enter"),
            (SessionPickerRename, "Enter"),
            (ModelPicker, "Enter"),
            (CommandPalette, "Enter")
        ]
    ),
    spec!(
        Cancel,
        "Cancel or close",
        "Cancel the current mode without applying changes",
        false,
        [
            Help,
            Notifications,
            QuestionOptions,
            QuestionCustom,
            QuestionNumber,
            QuestionInspect,
            ScheduleForm,
            ConfigEditor,
            Permissions,
            PermissionEditor,
            SubagentTree,
            TaskPlan,
            SessionPickerBrowse,
            SessionPickerRename,
            SessionPickerPinning,
            ModelPicker,
            CommandPalette
        ],
        [
            (Help, "Esc; q; F1"),
            (Notifications, "Esc; q; F1"),
            (QuestionOptions, "Esc"),
            (QuestionCustom, "Esc"),
            (QuestionNumber, "Esc"),
            (QuestionInspect, "Esc; v"),
            (ScheduleForm, "Esc"),
            (ConfigEditor, "Esc"),
            (Permissions, "Esc"),
            (PermissionEditor, "Esc"),
            (SubagentTree, "Esc; q"),
            (TaskPlan, "Esc; q"),
            (SessionPickerBrowse, "Esc"),
            (SessionPickerRename, "Esc"),
            (SessionPickerPinning, "Esc"),
            (ModelPicker, "Esc"),
            (CommandPalette, "Esc; Ctrl+K")
        ]
    ),
    spec!(
        Backspace,
        "Delete previous character",
        "Remove the previous character from the focused input",
        false,
        [
            QuestionCustom,
            QuestionNumber,
            ScheduleForm,
            SessionPickerBrowse,
            SessionPickerRename,
            ModelPicker,
            CommandPalette
        ],
        [
            (QuestionCustom, "Backspace"),
            (QuestionNumber, "Backspace"),
            (ScheduleForm, "Backspace"),
            (SessionPickerBrowse, "Backspace"),
            (SessionPickerRename, "Backspace"),
            (ModelPicker, "Backspace"),
            (CommandPalette, "Backspace")
        ]
    ),
    spec!(
        Refresh,
        "Refresh",
        "Reload the current catalog or retry its last request",
        false,
        [
            Sessions,
            Mcp,
            SubagentTree,
            TaskPlan,
            SessionPickerBrowse,
            SessionPickerRename,
            SessionPickerPinning,
            ModelPicker,
            CommandPalette,
            Permissions,
            PermissionModeConfirm
        ],
        [
            (Sessions, "r"),
            (Mcp, "r"),
            (SubagentTree, "r; Ctrl+R"),
            (TaskPlan, "r; Ctrl+R"),
            (SessionPickerBrowse, "Ctrl+R"),
            (SessionPickerRename, "Ctrl+R"),
            (SessionPickerPinning, "Ctrl+R"),
            (ModelPicker, "Ctrl+R"),
            (CommandPalette, "Ctrl+R"),
            (Permissions, "r"),
            (PermissionModeConfirm, "r")
        ]
    ),
    spec!(
        ClearInput,
        "Clear input",
        "Clear the focused search query",
        false,
        [SessionPickerBrowse, ModelPicker, CommandPalette],
        [
            (SessionPickerBrowse, "Ctrl+U"),
            (ModelPicker, "Ctrl+U"),
            (CommandPalette, "Ctrl+U")
        ]
    ),
    spec!(
        PreviousReasoningEffort,
        "Previous reasoning effort",
        "Select the previous canonical reasoning profile",
        false,
        [ModelPicker],
        [(ModelPicker, "Left")]
    ),
    spec!(
        NextReasoningEffort,
        "Next reasoning effort",
        "Select the next canonical reasoning profile",
        false,
        [ModelPicker],
        [(ModelPicker, "Right")]
    ),
    spec!(
        Confirm,
        "Confirm",
        "Accept the pending confirmation",
        false,
        [
            ServeOffer,
            SessionDeleteConfirm,
            ScheduleDeleteConfirm,
            PermissionDeleteConfirm,
            PermissionRuleConfirm,
            PermissionModeConfirm
        ],
        [
            (ServeOffer, "y; Enter"),
            (SessionDeleteConfirm, "y; Enter"),
            (ScheduleDeleteConfirm, "y; Enter"),
            (PermissionDeleteConfirm, "y; Enter"),
            (PermissionRuleConfirm, "y; Enter"),
            (PermissionModeConfirm, "y; Enter")
        ]
    ),
    spec!(
        Reject,
        "Reject",
        "Decline or cancel the pending confirmation",
        false,
        [
            ServeOffer,
            SessionDeleteConfirm,
            ScheduleDeleteConfirm,
            PermissionDeleteConfirm,
            PermissionRuleConfirm,
            PermissionModeConfirm
        ],
        [
            (ServeOffer, "n; Esc"),
            (SessionDeleteConfirm, "n; Esc"),
            (ScheduleDeleteConfirm, "n; Esc"),
            (PermissionDeleteConfirm, "n; Esc"),
            (PermissionRuleConfirm, "n; Esc"),
            (PermissionModeConfirm, "n; Esc")
        ]
    ),
    spec!(
        OpenSlashPalette,
        "Slash commands",
        "Open command discovery from an empty Chat composer",
        false,
        [Chat],
        [(Chat, "/")]
    ),
    spec!(
        SendMessage,
        "Send message",
        "Send the current Chat composer draft",
        false,
        [Chat],
        [(Chat, "Enter")]
    ),
    spec!(
        InsertNewline,
        "Insert newline",
        "Insert a newline without sending the Chat draft",
        false,
        [Chat],
        [(Chat, "Alt+Enter; Shift+Enter")]
    ),
    spec!(
        ScrollTranscriptUp,
        "Scroll transcript up",
        "Scroll the Chat transcript upward",
        false,
        [Chat],
        [(Chat, "PageUp; Alt+Up")]
    ),
    spec!(
        ScrollTranscriptDown,
        "Scroll transcript down",
        "Scroll the Chat transcript downward",
        false,
        [Chat],
        [(Chat, "PageDown; Alt+Down")]
    ),
    spec!(
        ScrollTranscriptTop,
        "Transcript top",
        "Jump to the top of the Chat transcript",
        false,
        [Chat],
        [(Chat, "Ctrl+Home")]
    ),
    spec!(
        ScrollTranscriptBottom,
        "Transcript bottom",
        "Jump to the newest Chat output",
        false,
        [Chat],
        [(Chat, "Ctrl+End; Ctrl+G")]
    ),
    spec!(
        FocusConversationBlocks,
        "Focus conversation blocks",
        "Move keyboard focus from the composer into rendered blocks",
        false,
        [Chat],
        [(Chat, "Ctrl+B")]
    ),
    spec!(
        ExitConversationBlocks,
        "Focus composer",
        "Return keyboard focus to the Chat composer",
        false,
        [ConversationBlock],
        [(ConversationBlock, "Esc; Ctrl+B")]
    ),
    spec!(
        PreviousConversationBlock,
        "Previous block",
        "Move focus to the previous conversation block",
        false,
        [ConversationBlock],
        [(ConversationBlock, "Up")]
    ),
    spec!(
        NextConversationBlock,
        "Next block",
        "Move focus to the next conversation block",
        false,
        [ConversationBlock],
        [(ConversationBlock, "Down")]
    ),
    spec!(
        ScrollBlockUp,
        "Scroll block up",
        "Scroll the focused detail block upward",
        false,
        [ConversationBlock],
        [(ConversationBlock, "k")]
    ),
    spec!(
        ScrollBlockDown,
        "Scroll block down",
        "Scroll the focused detail block downward",
        false,
        [ConversationBlock],
        [(ConversationBlock, "j")]
    ),
    spec!(
        ScrollBlockPageUp,
        "Page block up",
        "Scroll the focused detail block up by one viewport",
        false,
        [ConversationBlock],
        [(ConversationBlock, "PageUp")]
    ),
    spec!(
        ScrollBlockPageDown,
        "Page block down",
        "Scroll the focused detail block down by one viewport",
        false,
        [ConversationBlock],
        [(ConversationBlock, "PageDown")]
    ),
    spec!(
        PreviousDiffHunk,
        "Previous diff hunk",
        "Jump to the previous hunk in the focused file change",
        false,
        [ConversationBlock],
        [(ConversationBlock, "[")]
    ),
    spec!(
        NextDiffHunk,
        "Next diff hunk",
        "Jump to the next hunk in the focused file change",
        false,
        [ConversationBlock],
        [(ConversationBlock, "]")]
    ),
    spec!(
        ToggleDiffWrap,
        "Toggle diff wrapping",
        "Wrap or clip long lines in the focused diff without changing copied content",
        false,
        [ConversationBlock],
        [(ConversationBlock, "w")]
    ),
    spec!(
        CopyValue,
        "Copy exact value",
        "Copy the focused value through OSC 52",
        false,
        [ConversationBlock, QuestionOptions, QuestionInspect],
        [
            (ConversationBlock, "y"),
            (QuestionOptions, "y"),
            (QuestionInspect, "y")
        ]
    ),
    spec!(
        InspectValue,
        "Inspect full value",
        "Open the full question/value inspector",
        false,
        [QuestionOptions, QuestionCustom],
        [(QuestionOptions, "v"), (QuestionCustom, "Ctrl+V")]
    ),
    spec!(
        ToggleInspectorPane,
        "Toggle inspected value",
        "Switch between the question and selected option",
        false,
        [QuestionInspect, TaskPlan],
        [(QuestionInspect, "Tab"), (TaskPlan, "Tab")]
    ),
    spec!(
        CustomAnswer,
        "Custom answer",
        "Enter a free-text answer",
        false,
        [QuestionOptions],
        [(QuestionOptions, "c")]
    ),
    spec!(
        NumberAnswer,
        "Option by number",
        "Enter a multi-digit option number",
        false,
        [QuestionOptions],
        [(QuestionOptions, "g")]
    ),
    spec!(
        QuickAnswer1,
        "Answer option 1",
        "Submit question option 1",
        false,
        [QuestionOptions],
        [(QuestionOptions, "1")]
    ),
    spec!(
        QuickAnswer2,
        "Answer option 2",
        "Submit question option 2",
        false,
        [QuestionOptions],
        [(QuestionOptions, "2")]
    ),
    spec!(
        QuickAnswer3,
        "Answer option 3",
        "Submit question option 3",
        false,
        [QuestionOptions],
        [(QuestionOptions, "3")]
    ),
    spec!(
        QuickAnswer4,
        "Answer option 4",
        "Submit question option 4",
        false,
        [QuestionOptions],
        [(QuestionOptions, "4")]
    ),
    spec!(
        QuickAnswer5,
        "Answer option 5",
        "Submit question option 5",
        false,
        [QuestionOptions],
        [(QuestionOptions, "5")]
    ),
    spec!(
        QuickAnswer6,
        "Answer option 6",
        "Submit question option 6",
        false,
        [QuestionOptions],
        [(QuestionOptions, "6")]
    ),
    spec!(
        QuickAnswer7,
        "Answer option 7",
        "Submit question option 7",
        false,
        [QuestionOptions],
        [(QuestionOptions, "7")]
    ),
    spec!(
        QuickAnswer8,
        "Answer option 8",
        "Submit question option 8",
        false,
        [QuestionOptions],
        [(QuestionOptions, "8")]
    ),
    spec!(
        QuickAnswer9,
        "Answer option 9",
        "Submit question option 9",
        false,
        [QuestionOptions],
        [(QuestionOptions, "9")]
    ),
    spec!(
        DeleteSelection,
        "Delete selection",
        "Open a destructive confirmation for the selected item",
        false,
        [Sessions, Schedules, SessionPickerBrowse, Permissions],
        [
            (Sessions, "d"),
            (Schedules, "d"),
            (SessionPickerBrowse, "Delete; Ctrl+D"),
            (Permissions, "d; Delete")
        ]
    ),
    spec!(
        NextPage,
        "Next page",
        "Load the next page of sessions",
        false,
        [Sessions],
        [(Sessions, "]")]
    ),
    spec!(
        PreviousPage,
        "Previous page",
        "Load the previous page of sessions",
        false,
        [Sessions],
        [(Sessions, "[")]
    ),
    spec!(
        ShowTools,
        "Show tools",
        "Load tools for the selected MCP server",
        false,
        [Mcp],
        [(Mcp, "t")]
    ),
    spec!(
        NewSchedule,
        "New schedule",
        "Open the new schedule form",
        false,
        [Schedules],
        [(Schedules, "n")]
    ),
    spec!(
        RunSchedule,
        "Run schedule now",
        "Trigger the selected schedule immediately",
        false,
        [Schedules],
        [(Schedules, "r")]
    ),
    spec!(
        NextField,
        "Next field",
        "Move focus to the next form field",
        false,
        [ScheduleForm],
        [(ScheduleForm, "Tab; Down")]
    ),
    spec!(
        PreviousField,
        "Previous field",
        "Move focus to the previous form field",
        false,
        [ScheduleForm],
        [(ScheduleForm, "Shift+Tab; Up")]
    ),
    spec!(
        EditConfig,
        "Edit config",
        "Open the raw JSON configuration editor",
        false,
        [Config],
        [(Config, "e")]
    ),
    spec!(
        SaveConfig,
        "Save config",
        "Validate and save the configuration editor buffer",
        false,
        [ConfigEditor],
        [(ConfigEditor, "F2; Ctrl+S; Leader s")]
    ),
    spec!(
        OpenPermissionPolicy,
        "Open permission policy",
        "Open the typed permission rules and runtime grants view",
        true,
        [Global, Config],
        [(Config, "p")]
    ),
    spec!(
        TogglePermissionBypass,
        "Toggle session bypass",
        "Review and confirm a per-session bypass posture change",
        true,
        [Global, Permissions],
        [(Permissions, "b")]
    ),
    spec!(
        NewPermissionRule,
        "New permission rule",
        "Create a typed permission rule from exact JSON",
        false,
        [Permissions],
        [(Permissions, "n")]
    ),
    spec!(
        EditPermissionRule,
        "Edit permission rule",
        "Edit the selected typed permission rule",
        false,
        [Permissions],
        [(Permissions, "e")]
    ),
    spec!(
        DiagnosePermission,
        "Diagnose permission",
        "Evaluate a typed permission request without consuming a grant",
        false,
        [Permissions],
        [(Permissions, "x")]
    ),
    spec!(
        SavePermissionForm,
        "Submit permission form",
        "Validate and submit the typed permission JSON",
        false,
        [PermissionEditor],
        [(PermissionEditor, "F2; Ctrl+S; Leader s")]
    ),
    spec!(
        RenameSession,
        "Rename session",
        "Rename the selected session",
        false,
        [SessionPickerBrowse],
        [(SessionPickerBrowse, "F2")]
    ),
    spec!(
        ToggleSessionPin,
        "Pin or unpin session",
        "Toggle the selected session's pinned state",
        false,
        [SessionPickerBrowse],
        [(SessionPickerBrowse, "F3")]
    ),
    spec!(
        LoadMore,
        "Load more",
        "Load the next session-picker page",
        false,
        [SessionPickerBrowse],
        [(SessionPickerBrowse, "PageDown; ]")]
    ),
    spec!(
        ExpandTreeNode,
        "Expand tree node",
        "Expand the selected child-session branch or enter its first child",
        false,
        [SubagentTree],
        [(SubagentTree, "Right; l")]
    ),
    spec!(
        CollapseTreeNode,
        "Collapse tree node",
        "Collapse the selected branch or move to its parent",
        false,
        [SubagentTree],
        [(SubagentTree, "Left; h")]
    ),
    spec!(
        OpenPendingRequest,
        "Open pending child request",
        "Jump to the exact selected child's clarification or permission request",
        false,
        [SubagentTree],
        [(SubagentTree, "p")]
    ),
];

impl ActionId {
    pub(crate) fn spec(self) -> &'static ActionSpec {
        ACTION_SPECS
            .iter()
            .find(|spec| spec.id == self)
            .expect("every ActionId must have one ActionSpec")
    }

    pub(crate) fn label(self) -> &'static str {
        self.spec().label
    }

    pub(crate) fn description(self) -> &'static str {
        self.spec().description
    }

    pub(crate) fn key(self) -> String {
        serde_json::to_value(self)
            .ok()
            .and_then(|value| value.as_str().map(str::to_string))
            .expect("ActionId serialization must stay a string")
    }

    pub(crate) fn palette_actions() -> impl Iterator<Item = ActionId> {
        ACTION_SPECS
            .iter()
            .filter(|spec| spec.palette)
            .map(|spec| spec.id)
    }

    pub(crate) fn availability(self) -> ActionAvailability {
        self.spec().availability
    }

    /// Whether holding the physical key may safely invoke this action more
    /// than once.  Text entry is handled separately by the focused widget;
    /// confirmations, submissions, mutations, and application lifecycle
    /// actions deliberately remain press-only.
    pub(crate) fn repeatable(self) -> bool {
        matches!(
            self,
            Self::NavigateUp
                | Self::NavigateDown
                | Self::PageUp
                | Self::PageDown
                | Self::ScrollTranscriptUp
                | Self::ScrollTranscriptDown
                | Self::ScrollBlockUp
                | Self::ScrollBlockDown
                | Self::ScrollBlockPageUp
                | Self::ScrollBlockPageDown
                | Self::PreviousDiffHunk
                | Self::NextDiffHunk
                | Self::Backspace
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct KeyStroke {
    code: KeyCode,
    modifiers: KeyModifiers,
}

impl KeyStroke {
    fn from_event(event: KeyEvent) -> Self {
        // Keep every modifier reported by crossterm.  SUPER/HYPER/META are not
        // configurable today, but silently discarding them would turn e.g.
        // Super+Y into plain `y` and could confirm a destructive dialog.
        let mut modifiers = event.modifiers;
        let code = match event.code {
            KeyCode::BackTab => {
                modifiers.remove(KeyModifiers::SHIFT);
                KeyCode::BackTab
            }
            KeyCode::Char(character) => {
                let character = character.to_ascii_lowercase();
                if !character.is_ascii_alphabetic() {
                    modifiers.remove(KeyModifiers::SHIFT);
                }
                KeyCode::Char(character)
            }
            code => code,
        };
        Self { code, modifiers }
    }

    fn display(&self) -> String {
        let mut parts = Vec::new();
        if self.modifiers.contains(KeyModifiers::CONTROL) {
            parts.push("Ctrl".to_string());
        }
        if self.modifiers.contains(KeyModifiers::ALT) {
            parts.push("Alt".to_string());
        }
        if self.modifiers.contains(KeyModifiers::SHIFT) {
            parts.push("Shift".to_string());
        }
        if self.modifiers.contains(KeyModifiers::SUPER) {
            parts.push("Super".to_string());
        }
        if self.modifiers.contains(KeyModifiers::HYPER) {
            parts.push("Hyper".to_string());
        }
        if self.modifiers.contains(KeyModifiers::META) {
            parts.push("Meta".to_string());
        }
        parts.push(match self.code {
            KeyCode::Backspace => "Backspace".to_string(),
            KeyCode::Enter => "Enter".to_string(),
            KeyCode::Left => "Left".to_string(),
            KeyCode::Right => "Right".to_string(),
            KeyCode::Up => "Up".to_string(),
            KeyCode::Down => "Down".to_string(),
            KeyCode::Home => "Home".to_string(),
            KeyCode::End => "End".to_string(),
            KeyCode::PageUp => "PageUp".to_string(),
            KeyCode::PageDown => "PageDown".to_string(),
            KeyCode::Tab => "Tab".to_string(),
            KeyCode::BackTab => "Shift+Tab".to_string(),
            KeyCode::Delete => "Delete".to_string(),
            KeyCode::Insert => "Insert".to_string(),
            KeyCode::F(number) => format!("F{number}"),
            KeyCode::Char(' ') => "Space".to_string(),
            KeyCode::Char(character)
                if character.is_ascii_alphabetic() && !self.modifiers.is_empty() =>
            {
                character.to_ascii_uppercase().to_string()
            }
            KeyCode::Char(character) => character.to_string(),
            KeyCode::Esc => "Esc".to_string(),
            KeyCode::Null => "Null".to_string(),
            KeyCode::CapsLock => "CapsLock".to_string(),
            KeyCode::ScrollLock => "ScrollLock".to_string(),
            KeyCode::NumLock => "NumLock".to_string(),
            KeyCode::PrintScreen => "PrintScreen".to_string(),
            KeyCode::Pause => "Pause".to_string(),
            KeyCode::Menu => "Menu".to_string(),
            KeyCode::KeypadBegin => "KeypadBegin".to_string(),
            KeyCode::Media(_) | KeyCode::Modifier(_) => "Unsupported".to_string(),
        });
        parts.join("+")
    }

    fn is_plain_printable(&self) -> bool {
        (self.modifiers - KeyModifiers::SHIFT).is_empty() && matches!(self.code, KeyCode::Char(_))
    }

    fn is_xon_xoff_or_signal(&self) -> bool {
        self.modifiers == KeyModifiers::CONTROL
            && matches!(self.code, KeyCode::Char('q' | 's' | 'z'))
    }

    fn is_protocol_dependent(&self) -> bool {
        (self.code == KeyCode::Enter && self.modifiers == KeyModifiers::SHIFT)
            || (self.code == KeyCode::Char('?') && self.modifiers == KeyModifiers::CONTROL)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct KeySequence(Vec<KeyStroke>);

impl KeySequence {
    fn display(&self) -> String {
        self.0
            .iter()
            .map(KeyStroke::display)
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn is_prefix_of(&self, other: &Self) -> bool {
        self.0.len() <= other.0.len() && other.0.starts_with(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BindingSource {
    Default,
    Custom,
}

#[derive(Clone, Debug)]
struct Binding {
    context: ActionContext,
    action: ActionId,
    sequence: KeySequence,
    source: BindingSource,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingSequence {
    contexts: Vec<ActionContext>,
    focus_contexts: Vec<ActionContext>,
    sequence: KeySequence,
    started_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KeyResolution {
    Action {
        context: ActionContext,
        action: ActionId,
    },
    Pending(String),
    Cancelled(String),
    NoMatch,
}

#[derive(Clone, Debug)]
pub(crate) struct HelpEntry {
    pub(crate) keys: String,
    pub(crate) description: String,
}

#[derive(Clone, Debug)]
pub(crate) struct Keymap {
    bindings: Vec<Binding>,
    leader: KeyStroke,
    timeout: Duration,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeymapConfig {
    #[serde(default = "default_keymap_version")]
    version: u32,
    #[serde(default)]
    leader: Option<String>,
    #[serde(default)]
    leader_timeout_ms: Option<u64>,
    #[serde(default)]
    bindings: Vec<KeymapOverride>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeymapOverride {
    context: ActionContext,
    action: ActionId,
    #[serde(default)]
    keys: Vec<String>,
    #[serde(default)]
    unbind: bool,
}

fn default_keymap_version() -> u32 {
    1
}

#[derive(Debug)]
pub(crate) struct KeymapError(String);

impl fmt::Display for KeymapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for KeymapError {}

impl Default for Keymap {
    fn default() -> Self {
        Self::build(None).expect("built-in keymap must remain valid")
    }
}

impl Keymap {
    pub(crate) fn load(path: Option<&Path>) -> (Self, Option<String>) {
        let Some(path) = path else {
            return (Self::default(), None);
        };
        let loaded = std::fs::read_to_string(path)
            .map_err(|error| KeymapError(format!("cannot read {}: {error}", path.display())))
            .and_then(|source| {
                Self::from_json(&source)
                    .map_err(|error| KeymapError(format!("{}: {error}", path.display())))
            });
        match loaded {
            Ok(keymap) => (keymap, None),
            Err(error) => (
                Self::default(),
                Some(format!(
                    "Invalid TUI keymap ({error}); using conflict-safe defaults"
                )),
            ),
        }
    }

    pub(crate) fn from_json(source: &str) -> Result<Self, KeymapError> {
        let config: KeymapConfig = serde_json::from_str(source)
            .map_err(|error| KeymapError(format!("JSON error: {error}")))?;
        Self::build(Some(config))
    }

    fn build(config: Option<KeymapConfig>) -> Result<Self, KeymapError> {
        if config.as_ref().is_some_and(|config| config.version != 1) {
            let version = config.as_ref().map(|config| config.version).unwrap_or(1);
            return Err(KeymapError(format!(
                "unsupported keymap version {} (expected 1)",
                version
            )));
        }
        let leader_text = config
            .as_ref()
            .and_then(|config| config.leader.as_deref())
            .unwrap_or(DEFAULT_LEADER);
        let leader = parse_stroke(leader_text)
            .map_err(|error| KeymapError(format!("invalid leader '{leader_text}': {error}")))?;
        if leader.is_plain_printable() {
            return Err(KeymapError(
                "leader must use Ctrl/Alt or a non-printable key so Chat typing stays safe"
                    .to_string(),
            ));
        }
        if config.is_some() && leader.is_xon_xoff_or_signal() {
            return Err(KeymapError(format!(
                "leader '{}' is reserved by common terminal flow-control/signal handling",
                leader.display()
            )));
        }
        if config.is_some() && leader.is_protocol_dependent() {
            return Err(KeymapError(format!(
                "leader '{}' depends on enhanced terminal key reporting; use Ctrl/Alt with a portable key or a function key",
                leader.display()
            )));
        }

        let timeout_ms = config
            .as_ref()
            .and_then(|config| config.leader_timeout_ms)
            .unwrap_or(DEFAULT_LEADER_TIMEOUT_MS);
        if !(MIN_LEADER_TIMEOUT_MS..=MAX_LEADER_TIMEOUT_MS).contains(&timeout_ms) {
            return Err(KeymapError(format!(
                "leader_timeout_ms must be between {MIN_LEADER_TIMEOUT_MS} and {MAX_LEADER_TIMEOUT_MS}"
            )));
        }

        let mut bindings = Vec::new();
        for spec in ACTION_SPECS {
            for default in spec.defaults {
                for sequence in split_binding_list(default.keys)? {
                    bindings.push(Binding {
                        context: default.context,
                        action: spec.id,
                        sequence: parse_sequence(&sequence, &leader)?,
                        source: BindingSource::Default,
                    });
                }
            }
        }

        if let Some(config) = config {
            let mut overridden = HashSet::new();
            for entry in config.bindings {
                if !overridden.insert((entry.context, entry.action)) {
                    return Err(KeymapError(format!(
                        "duplicate override for {} / {}",
                        entry.context.label(),
                        entry.action.label()
                    )));
                }
                if !entry.action.spec().contexts.contains(&entry.context) {
                    return Err(KeymapError(format!(
                        "action '{}' is not valid in context '{}'",
                        entry.action.label(),
                        entry.context.label()
                    )));
                }
                if entry.unbind && !entry.keys.is_empty() {
                    return Err(KeymapError(format!(
                        "{} / {} cannot set both unbind=true and keys",
                        entry.context.label(),
                        entry.action.label()
                    )));
                }
                if !entry.unbind && entry.keys.is_empty() {
                    return Err(KeymapError(format!(
                        "{} / {} needs at least one key or unbind=true",
                        entry.context.label(),
                        entry.action.label()
                    )));
                }
                bindings.retain(|binding| {
                    binding.context != entry.context || binding.action != entry.action
                });
                for sequence in entry.keys {
                    let sequence = parse_sequence(&sequence, &leader)?;
                    if sequence.0.iter().any(KeyStroke::is_xon_xoff_or_signal) {
                        return Err(KeymapError(format!(
                            "{} / {} uses reserved sequence '{}'; use a leader or function-key fallback",
                            entry.context.label(),
                            entry.action.label(),
                            sequence.display()
                        )));
                    }
                    bindings.push(Binding {
                        context: entry.context,
                        action: entry.action,
                        sequence,
                        source: BindingSource::Custom,
                    });
                }
            }
        }

        let keymap = Self {
            bindings,
            leader,
            timeout: Duration::from_millis(timeout_ms),
        };
        keymap.validate()?;
        Ok(keymap)
    }

    fn validate(&self) -> Result<(), KeymapError> {
        for (index, binding) in self.bindings.iter().enumerate() {
            if !binding.action.spec().contexts.contains(&binding.context) {
                return Err(KeymapError(format!(
                    "{} is not valid in {}",
                    binding.action.label(),
                    binding.context.label()
                )));
            }
            if binding.context == ActionContext::Global
                && binding
                    .sequence
                    .0
                    .first()
                    .is_some_and(KeyStroke::is_plain_printable)
            {
                return Err(KeymapError(format!(
                    "global binding '{}' starts with a printable key and would steal ordinary Chat text; start it with Leader, Ctrl, Alt, or a function key",
                    binding.sequence.display(),
                )));
            }
            for other in self.bindings.iter().skip(index + 1) {
                if binding.context != other.context {
                    continue;
                }
                if binding.sequence == other.sequence {
                    return Err(KeymapError(format!(
                        "conflict in {}: '{}' binds both '{}' and '{}'",
                        binding.context.label(),
                        binding.sequence.display(),
                        binding.action.label(),
                        other.action.label()
                    )));
                }
                if binding.sequence.is_prefix_of(&other.sequence)
                    || other.sequence.is_prefix_of(&binding.sequence)
                {
                    return Err(KeymapError(format!(
                        "unreachable prefix in {}: '{}' conflicts with '{}'",
                        binding.context.label(),
                        binding.sequence.display(),
                        other.sequence.display()
                    )));
                }
            }
        }

        for quit in self.bindings.iter().filter(|binding| {
            binding.context == ActionContext::Global
                && binding.action == ActionId::QuitOrStop
                && binding.sequence.0.len() == 1
        }) {
            if let Some(binding) = self.bindings.iter().find(|binding| {
                binding.sequence.0.len() > 1
                    && binding
                        .sequence
                        .0
                        .iter()
                        .any(|stroke| Some(stroke) == quit.sequence.0.first())
            }) {
                return Err(KeymapError(format!(
                    "unreachable binding in {}: '{}' contains single-stroke global quit '{}', which always preempts pending and focused actions",
                    binding.context.label(),
                    binding.sequence.display(),
                    quit.sequence.display(),
                )));
            }
        }

        if let Some(binding) = self.bindings.iter().find(|binding| {
            binding
                .sequence
                .0
                .iter()
                .skip(1)
                .any(|stroke| stroke.code == KeyCode::Esc && stroke.modifiers.is_empty())
        }) {
            return Err(KeymapError(format!(
                "unreachable binding in {}: '{}' contains Esc after the first stroke; Esc always cancels a pending sequence",
                binding.context.label(),
                binding.sequence.display(),
            )));
        }

        for (context, action) in [
            (ActionContext::Global, ActionId::QuitOrStop),
            (ActionContext::Global, ActionId::ShowHelp),
            (ActionContext::ServeOffer, ActionId::Confirm),
            (ActionContext::ServeOffer, ActionId::Reject),
            (ActionContext::QuestionOptions, ActionId::Activate),
            (ActionContext::QuestionOptions, ActionId::Cancel),
            (ActionContext::QuestionCustom, ActionId::Activate),
            (ActionContext::QuestionCustom, ActionId::Cancel),
            (ActionContext::QuestionNumber, ActionId::Activate),
            (ActionContext::QuestionNumber, ActionId::Cancel),
            (ActionContext::QuestionInspect, ActionId::Cancel),
            (ActionContext::SessionDeleteConfirm, ActionId::Confirm),
            (ActionContext::SessionDeleteConfirm, ActionId::Reject),
            (ActionContext::ScheduleDeleteConfirm, ActionId::Confirm),
            (ActionContext::ScheduleDeleteConfirm, ActionId::Reject),
        ] {
            if !self
                .bindings
                .iter()
                .any(|binding| binding.context == context && binding.action == action)
            {
                return Err(KeymapError(format!(
                    "required action '{}' would be unreachable in {}",
                    action.label(),
                    context.label()
                )));
            }
        }

        let mut checked = HashSet::new();
        for binding in self
            .bindings
            .iter()
            .filter(|binding| binding.source == BindingSource::Custom)
        {
            if !checked.insert((binding.context, binding.action)) {
                continue;
            }
            let group: Vec<&Binding> = self
                .bindings
                .iter()
                .filter(|candidate| {
                    candidate.context == binding.context && candidate.action == binding.action
                })
                .collect();
            if !group.is_empty()
                && group.iter().all(|candidate| {
                    candidate
                        .sequence
                        .0
                        .iter()
                        .any(KeyStroke::is_protocol_dependent)
                })
            {
                return Err(KeymapError(format!(
                    "{} / {} relies only on terminal-protocol-dependent keys; add an Alt, leader, or function-key fallback",
                    binding.context.label(),
                    binding.action.label()
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn resolve(
        &self,
        contexts: &[ActionContext],
        pending: &mut Option<PendingSequence>,
        event: KeyEvent,
        now: Instant,
    ) -> KeyResolution {
        let stroke = KeyStroke::from_event(event);

        // A single-stroke global quit binding is the emergency escape hatch.
        // It preempts both a higher-context prefix on its first stroke and an
        // incomplete leader sequence instead of becoming a continuation such
        // as `Leader Ctrl+C`.
        if contexts.contains(&ActionContext::Global)
            && self.bindings.iter().any(|binding| {
                binding.context == ActionContext::Global
                    && binding.action == ActionId::QuitOrStop
                    && binding.sequence.0.as_slice() == std::slice::from_ref(&stroke)
            })
        {
            *pending = None;
            return KeyResolution::Action {
                context: ActionContext::Global,
                action: ActionId::QuitOrStop,
            };
        }

        if let Some(current) = pending.as_ref() {
            if current.focus_contexts != contexts {
                *pending = None;
                return KeyResolution::Cancelled(
                    "Key sequence cancelled because focus changed".to_string(),
                );
            }
            let candidate_contexts = current
                .contexts
                .iter()
                .copied()
                .filter(|context| contexts.contains(context))
                .collect::<Vec<_>>();
            if candidate_contexts.is_empty() {
                *pending = None;
                return KeyResolution::Cancelled(
                    "Key sequence cancelled because focus changed".to_string(),
                );
            }
            if stroke.code == KeyCode::Esc && stroke.modifiers.is_empty() {
                *pending = None;
                return KeyResolution::Cancelled("Key sequence cancelled".to_string());
            }
            if now.duration_since(current.started_at) < self.timeout {
                let mut sequence = current.sequence.clone();
                sequence.0.push(stroke);
                return self.resolve_sequence(
                    &candidate_contexts,
                    contexts,
                    sequence,
                    pending,
                    now,
                );
            }

            // The timer tick normally expires pending sequences, but an input
            // event can win the select race.  Reprocess that event as a fresh
            // key so the first composer character after a timeout is not lost.
            *pending = None;
        }

        let sequence = KeySequence(vec![stroke]);
        let candidate_contexts = contexts
            .iter()
            .copied()
            .filter(|context| {
                self.bindings.iter().any(|binding| {
                    binding.context == *context && sequence.is_prefix_of(&binding.sequence)
                })
            })
            .collect::<Vec<_>>();
        if !candidate_contexts.is_empty() {
            return self.resolve_sequence(&candidate_contexts, contexts, sequence, pending, now);
        }
        KeyResolution::NoMatch
    }

    fn resolve_sequence(
        &self,
        candidate_contexts: &[ActionContext],
        focus_contexts: &[ActionContext],
        sequence: KeySequence,
        pending: &mut Option<PendingSequence>,
        now: Instant,
    ) -> KeyResolution {
        let mut pending_contexts = Vec::new();
        for context in candidate_contexts {
            if let Some(binding) = self
                .bindings
                .iter()
                .find(|binding| binding.context == *context && binding.sequence == sequence)
            {
                // An exact binding wins only when no higher-priority context
                // still has a longer match.  This preserves modal/focused
                // precedence without losing shared leaders whose continuations
                // live in several contexts.
                if pending_contexts.is_empty() {
                    *pending = None;
                    return KeyResolution::Action {
                        context: *context,
                        action: binding.action,
                    };
                }
                continue;
            }
            if self.bindings.iter().any(|binding| {
                binding.context == *context
                    && sequence.is_prefix_of(&binding.sequence)
                    && binding.sequence != sequence
            }) {
                pending_contexts.push(*context);
            }
        }
        if !pending_contexts.is_empty() {
            let display = sequence.display();
            *pending = Some(PendingSequence {
                contexts: pending_contexts,
                focus_contexts: focus_contexts.to_vec(),
                sequence,
                started_at: now,
            });
            return KeyResolution::Pending(format!(
                "Leader {display} — waiting for next key (Esc cancels)"
            ));
        }
        let display = sequence.display();
        *pending = None;
        KeyResolution::Cancelled(format!("No action is bound to '{display}'"))
    }

    pub(crate) fn expire(
        &self,
        pending: &mut Option<PendingSequence>,
        now: Instant,
    ) -> Option<String> {
        let current = pending.as_ref()?;
        if now.duration_since(current.started_at) < self.timeout {
            return None;
        }
        let display = current.sequence.display();
        *pending = None;
        Some(format!("Key sequence '{display}' timed out"))
    }

    pub(crate) fn hint(&self, context: ActionContext, action: ActionId) -> String {
        let labels = self
            .bindings
            .iter()
            .filter(|binding| binding.context == context && binding.action == action)
            .map(|binding| binding.sequence.display())
            .collect::<Vec<_>>();
        if labels.is_empty() {
            "unbound".to_string()
        } else {
            labels.join("/")
        }
    }

    pub(crate) fn action_hint(&self, action: ActionId) -> Option<String> {
        let labels = self
            .bindings
            .iter()
            .filter(|binding| binding.action == action)
            .map(|binding| binding.sequence.display())
            .collect::<Vec<_>>();
        (!labels.is_empty()).then(|| labels.join("/"))
    }

    pub(crate) fn help_entries(&self) -> Vec<HelpEntry> {
        let mut entries = vec![HelpEntry {
            keys: self.leader.display(),
            description: "Global · Leader prefix for conflict-safe alternatives".to_string(),
        }];
        for context in ActionContext::ALL {
            for spec in ACTION_SPECS {
                let keys = self.hint(context, spec.id);
                if keys == "unbound" {
                    continue;
                }
                entries.push(HelpEntry {
                    keys,
                    description: format!("{} · {}", context.label(), spec.label),
                });
            }
        }
        entries
    }

    #[cfg(test)]
    fn leader_label(&self) -> String {
        self.leader.display()
    }
}

pub(crate) fn text_character(event: KeyEvent) -> Option<char> {
    if !(event.modifiers - KeyModifiers::SHIFT).is_empty() {
        return None;
    }
    match event.code {
        KeyCode::Char(character) => Some(character),
        _ => None,
    }
}

pub(crate) fn text_widget_input_allowed(event: KeyEvent) -> bool {
    !event
        .modifiers
        .intersects(KeyModifiers::SUPER | KeyModifiers::HYPER | KeyModifiers::META)
}

fn split_binding_list(bindings: &str) -> Result<Vec<String>, KeymapError> {
    let values = bindings
        .split(';')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if values.is_empty() {
        Err(KeymapError("binding list is empty".to_string()))
    } else {
        Ok(values)
    }
}

fn parse_sequence(value: &str, leader: &KeyStroke) -> Result<KeySequence, KeymapError> {
    let mut strokes = Vec::new();
    for token in value.split_whitespace() {
        if token.eq_ignore_ascii_case("leader") {
            strokes.push(leader.clone());
        } else {
            strokes.push(parse_stroke(token).map_err(|error| {
                KeymapError(format!("invalid key sequence '{value}': {error}"))
            })?);
        }
    }
    if strokes.is_empty() {
        return Err(KeymapError("key sequence is empty".to_string()));
    }
    Ok(KeySequence(strokes))
}

fn parse_stroke(value: &str) -> Result<KeyStroke, KeymapError> {
    let parts = value.split('+').collect::<Vec<_>>();
    let (code_name, modifier_names) = parts
        .split_last()
        .ok_or_else(|| KeymapError("key is empty".to_string()))?;
    if code_name.is_empty() {
        return Err(KeymapError("key code is empty".to_string()));
    }
    let mut modifiers = KeyModifiers::empty();
    for modifier in modifier_names {
        let flag = match modifier.to_ascii_lowercase().as_str() {
            "ctrl" | "control" => KeyModifiers::CONTROL,
            "alt" | "option" => KeyModifiers::ALT,
            "shift" => KeyModifiers::SHIFT,
            _ => {
                return Err(KeymapError(format!(
                    "unknown modifier '{modifier}' (use Ctrl, Alt, or Shift)"
                )))
            }
        };
        if modifiers.contains(flag) {
            return Err(KeymapError(format!("duplicate modifier '{modifier}'")));
        }
        modifiers.insert(flag);
    }

    let lowered = code_name.to_ascii_lowercase();
    let mut code = match lowered.as_str() {
        "backspace" => KeyCode::Backspace,
        "enter" | "return" => KeyCode::Enter,
        "left" => KeyCode::Left,
        "right" => KeyCode::Right,
        "up" => KeyCode::Up,
        "down" => KeyCode::Down,
        "home" => KeyCode::Home,
        "end" => KeyCode::End,
        "pageup" | "pgup" => KeyCode::PageUp,
        "pagedown" | "pgdown" => KeyCode::PageDown,
        "tab" if modifiers.contains(KeyModifiers::SHIFT) => {
            modifiers.remove(KeyModifiers::SHIFT);
            KeyCode::BackTab
        }
        "tab" => KeyCode::Tab,
        "delete" | "del" => KeyCode::Delete,
        "insert" | "ins" => KeyCode::Insert,
        "esc" | "escape" => KeyCode::Esc,
        "space" => KeyCode::Char(' '),
        "plus" => KeyCode::Char('+'),
        _ if lowered.starts_with('f') => {
            let number = lowered[1..]
                .parse::<u8>()
                .map_err(|_| KeymapError(format!("unknown key code '{code_name}'")))?;
            if !(1..=24).contains(&number) {
                return Err(KeymapError("function key must be F1..F24".to_string()));
            }
            KeyCode::F(number)
        }
        _ => {
            let mut characters = code_name.chars();
            let character = characters
                .next()
                .filter(|_| characters.next().is_none())
                .ok_or_else(|| KeymapError(format!("unknown key code '{code_name}'")))?;
            KeyCode::Char(character.to_ascii_lowercase())
        }
    };
    if let KeyCode::Char(character) = code {
        if !character.is_ascii_alphabetic() {
            modifiers.remove(KeyModifiers::SHIFT);
        }
        code = KeyCode::Char(character.to_ascii_lowercase());
    }
    Ok(KeyStroke { code, modifiers })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    #[test]
    fn every_action_has_exactly_one_registry_spec() {
        let unique = ACTION_SPECS
            .iter()
            .map(|spec| spec.id)
            .collect::<HashSet<_>>();
        assert_eq!(unique.len(), ACTION_SPECS.len());
        for action in unique {
            assert!(!action.label().is_empty());
            assert!(!action.description().is_empty());
        }
        assert_eq!(ActionId::SwitchTab1.key(), "switch-tab-1");
        assert_eq!(ActionId::QuickAnswer9.key(), "quick-answer-9");
    }

    #[test]
    fn every_context_has_resolvable_default_bindings() {
        let keymap = Keymap::default();
        for context in ActionContext::ALL {
            let context_bindings = keymap
                .bindings
                .iter()
                .filter(|binding| binding.context == context)
                .collect::<Vec<_>>();
            assert!(
                !context_bindings.is_empty(),
                "{} has no default bindings",
                context.label()
            );
            for binding in context_bindings {
                let mut pending = None;
                let now = Instant::now();
                let mut resolution = KeyResolution::NoMatch;
                for (index, stroke) in binding.sequence.0.iter().enumerate() {
                    resolution = keymap.resolve(
                        &[context],
                        &mut pending,
                        key(stroke.code, stroke.modifiers),
                        now + Duration::from_millis(index as u64),
                    );
                }
                assert_eq!(
                    resolution,
                    KeyResolution::Action {
                        context,
                        action: binding.action,
                    },
                    "{} did not resolve {}",
                    binding.sequence.display(),
                    binding.action.label()
                );
            }
        }
    }

    #[test]
    fn conversation_block_activate_remains_reachable() {
        let keymap = Keymap::default();
        let mut pending = None;
        assert_eq!(
            keymap.resolve(
                &[
                    ActionContext::ConversationBlock,
                    ActionContext::Chat,
                    ActionContext::Global
                ],
                &mut pending,
                key(KeyCode::Enter, KeyModifiers::empty()),
                Instant::now(),
            ),
            KeyResolution::Action {
                context: ActionContext::ConversationBlock,
                action: ActionId::Activate,
            }
        );
    }

    #[test]
    fn defaults_have_safe_leader_and_required_fallbacks() {
        let keymap = Keymap::default();
        assert_eq!(keymap.leader_label(), "Ctrl+\\");
        assert!(keymap
            .hint(ActionContext::Global, ActionId::ReopenPendingQuestion)
            .contains("Ctrl+\\ q"));
        assert!(keymap
            .hint(ActionContext::Global, ActionId::StopRun)
            .contains("Ctrl+\\ s"));
        assert!(keymap
            .hint(ActionContext::ConfigEditor, ActionId::SaveConfig)
            .contains("F2"));
        assert!(keymap
            .hint(ActionContext::Chat, ActionId::InsertNewline)
            .starts_with("Alt+Enter"));
    }

    #[test]
    fn leader_sequence_resolves_and_escape_cancels() {
        let keymap = Keymap::default();
        let now = Instant::now();
        let mut pending = None;
        let first = keymap.resolve(
            &[ActionContext::Global],
            &mut pending,
            key(KeyCode::Char('\\'), KeyModifiers::CONTROL),
            now,
        );
        assert!(matches!(first, KeyResolution::Pending(_)));
        let second = keymap.resolve(
            &[ActionContext::Global],
            &mut pending,
            key(KeyCode::Char('h'), KeyModifiers::empty()),
            now + Duration::from_millis(10),
        );
        assert_eq!(
            second,
            KeyResolution::Action {
                context: ActionContext::Global,
                action: ActionId::ShowHelp,
            }
        );

        let _ = keymap.resolve(
            &[ActionContext::Global],
            &mut pending,
            key(KeyCode::Char('\\'), KeyModifiers::CONTROL),
            now,
        );
        let cancelled = keymap.resolve(
            &[ActionContext::Global],
            &mut pending,
            key(KeyCode::Esc, KeyModifiers::empty()),
            now + Duration::from_millis(10),
        );
        assert!(matches!(cancelled, KeyResolution::Cancelled(_)));
        assert!(pending.is_none());
    }

    #[test]
    fn shared_leader_keeps_local_and_global_context_candidates() {
        let keymap = Keymap::default();
        let now = Instant::now();
        let contexts = [ActionContext::Chat, ActionContext::Global];

        let mut pending = None;
        assert!(matches!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('\\'), KeyModifiers::CONTROL),
                now,
            ),
            KeyResolution::Pending(_)
        ));
        assert_eq!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('h'), KeyModifiers::empty()),
                now,
            ),
            KeyResolution::Action {
                context: ActionContext::Global,
                action: ActionId::ShowHelp,
            }
        );

        assert!(matches!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('\\'), KeyModifiers::CONTROL),
                now,
            ),
            KeyResolution::Pending(_)
        ));
        assert_eq!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('x'), KeyModifiers::empty()),
                now,
            ),
            KeyResolution::Action {
                context: ActionContext::Chat,
                action: ActionId::ToggleDetails,
            }
        );
    }

    #[test]
    fn single_stroke_global_quit_preempts_a_pending_sequence() {
        let keymap = Keymap::default();
        let now = Instant::now();
        let contexts = [ActionContext::Chat, ActionContext::Global];
        let mut pending = None;
        assert!(matches!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('\\'), KeyModifiers::CONTROL),
                now,
            ),
            KeyResolution::Pending(_)
        ));

        assert_eq!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('c'), KeyModifiers::CONTROL),
                now + Duration::from_millis(10),
            ),
            KeyResolution::Action {
                context: ActionContext::Global,
                action: ActionId::QuitOrStop,
            }
        );
        assert!(pending.is_none());
    }

    #[test]
    fn single_stroke_global_quit_preempts_an_initial_focused_prefix() {
        // Build a deliberately unvalidated map to verify the resolver remains
        // safe even if a future config path bypasses startup validation.
        let mut keymap = Keymap::default();
        keymap.bindings.push(Binding {
            context: ActionContext::Chat,
            action: ActionId::InsertNewline,
            sequence: parse_sequence("Ctrl+C x", &keymap.leader).unwrap(),
            source: BindingSource::Custom,
        });
        let contexts = [ActionContext::Chat, ActionContext::Global];
        let now = Instant::now();
        let mut pending = None;
        assert_eq!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('c'), KeyModifiers::CONTROL),
                now,
            ),
            KeyResolution::Action {
                context: ActionContext::Global,
                action: ActionId::QuitOrStop,
            }
        );
        assert!(pending.is_none());
        assert_eq!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('x'), KeyModifiers::empty()),
                now + Duration::from_millis(10),
            ),
            KeyResolution::NoMatch,
            "the shadowed continuation must never fire after emergency quit"
        );
    }

    #[test]
    fn expired_sequence_reprocesses_the_current_key() {
        let keymap = Keymap::default();
        let now = Instant::now();
        let contexts = [ActionContext::Chat, ActionContext::Global];
        let mut pending = None;
        assert!(matches!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('\\'), KeyModifiers::CONTROL),
                now,
            ),
            KeyResolution::Pending(_)
        ));

        assert_eq!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('a'), KeyModifiers::empty()),
                now + Duration::from_millis(1_001),
            ),
            KeyResolution::NoMatch,
            "the first composer character after timeout must be handled fresh"
        );
        assert!(pending.is_none());
    }

    #[test]
    fn cross_context_prefixes_obey_focused_precedence() {
        let keymap = Keymap::from_json(
            r#"{"bindings":[
                {"context":"chat","action":"insert-newline","keys":["F2 x"]},
                {"context":"global","action":"show-help","keys":["F2"]}
            ]}"#,
        )
        .unwrap();
        let contexts = [ActionContext::Chat, ActionContext::Global];
        let now = Instant::now();
        let mut pending = None;
        assert!(matches!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::F(2), KeyModifiers::empty()),
                now,
            ),
            KeyResolution::Pending(_)
        ));
        assert_eq!(
            keymap.resolve(
                &contexts,
                &mut pending,
                key(KeyCode::Char('x'), KeyModifiers::empty()),
                now + Duration::from_millis(10),
            ),
            KeyResolution::Action {
                context: ActionContext::Chat,
                action: ActionId::InsertNewline,
            }
        );

        let reverse = Keymap::from_json(
            r#"{"bindings":[
                {"context":"chat","action":"insert-newline","keys":["F2"]},
                {"context":"global","action":"show-help","keys":["F2 x"]}
            ]}"#,
        )
        .unwrap();
        assert_eq!(
            reverse.resolve(
                &contexts,
                &mut None,
                key(KeyCode::F(2), KeyModifiers::empty()),
                now,
            ),
            KeyResolution::Action {
                context: ActionContext::Chat,
                action: ActionId::InsertNewline,
            }
        );

        let destructive = Keymap::from_json(
            r#"{"bindings":[
                {"context":"session-delete-confirm","action":"confirm","keys":["F3 y"]},
                {"context":"global","action":"show-help","keys":["F3"]}
            ]}"#,
        )
        .unwrap();
        let modal_contexts = [ActionContext::SessionDeleteConfirm, ActionContext::Global];
        let mut modal_pending = None;
        assert!(matches!(
            destructive.resolve(
                &modal_contexts,
                &mut modal_pending,
                key(KeyCode::F(3), KeyModifiers::empty()),
                now,
            ),
            KeyResolution::Pending(_)
        ));
        assert_eq!(
            destructive.resolve(
                &modal_contexts,
                &mut modal_pending,
                key(KeyCode::Char('y'), KeyModifiers::empty()),
                now + Duration::from_millis(10),
            ),
            KeyResolution::Action {
                context: ActionContext::SessionDeleteConfirm,
                action: ActionId::Confirm,
            }
        );
    }

    #[test]
    fn focus_change_cancels_a_pending_sequence_even_when_global_remains() {
        let keymap = Keymap::default();
        let now = Instant::now();
        let mut pending = None;
        assert!(matches!(
            keymap.resolve(
                &[ActionContext::Chat, ActionContext::Global],
                &mut pending,
                key(KeyCode::Char('\\'), KeyModifiers::CONTROL),
                now,
            ),
            KeyResolution::Pending(_)
        ));

        let result = keymap.resolve(
            &[ActionContext::QuestionOptions, ActionContext::Global],
            &mut pending,
            key(KeyCode::Char('h'), KeyModifiers::empty()),
            now + Duration::from_millis(10),
        );
        assert!(
            matches!(result, KeyResolution::Cancelled(message) if message.contains("focus changed"))
        );
        assert!(pending.is_none());
    }

    #[test]
    fn leader_timeout_is_bounded_and_reported() {
        let keymap = Keymap::default();
        let now = Instant::now();
        let mut pending = None;
        let _ = keymap.resolve(
            &[ActionContext::Global],
            &mut pending,
            key(KeyCode::Char('\\'), KeyModifiers::CONTROL),
            now,
        );
        let message = keymap
            .expire(&mut pending, now + Duration::from_millis(1_001))
            .expect("sequence should expire");
        assert!(message.contains("timed out"));
        assert!(pending.is_none());
    }

    #[test]
    fn context_precedence_keeps_question_digits_out_of_navigation() {
        let keymap = Keymap::default();
        let mut pending = None;
        let resolved = keymap.resolve(
            &[
                ActionContext::QuestionOptions,
                ActionContext::Navigation,
                ActionContext::Global,
            ],
            &mut pending,
            key(KeyCode::Char('3'), KeyModifiers::empty()),
            Instant::now(),
        );
        assert_eq!(
            resolved,
            KeyResolution::Action {
                context: ActionContext::QuestionOptions,
                action: ActionId::QuickAnswer3,
            }
        );
    }

    #[test]
    fn custom_remap_and_unbind_replace_only_one_context() {
        let keymap = Keymap::from_json(
            r#"{
                "leader": "Alt+Space",
                "leader_timeout_ms": 750,
                "bindings": [
                    {"context":"global", "action":"show-notifications", "keys":["F8", "Leader l"]},
                    {"context":"navigation", "action":"show-help", "unbind":true}
                ]
            }"#,
        )
        .unwrap();
        assert_eq!(
            keymap.hint(ActionContext::Global, ActionId::ShowNotifications),
            "F8/Alt+Space l"
        );
        assert_eq!(
            keymap.hint(ActionContext::Navigation, ActionId::ShowHelp),
            "unbound"
        );
        assert!(keymap
            .hint(ActionContext::Global, ActionId::ShowHelp)
            .contains("F1"));
    }

    #[test]
    fn unsupported_config_version_is_actionable() {
        let error = Keymap::from_json(r#"{"version":2}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported keymap version 2"));
        assert!(error.contains("expected 1"));
    }

    #[test]
    fn invalid_maps_reject_conflicts_reserved_and_unreachable_required_actions() {
        let conflict = Keymap::from_json(
            r#"{"bindings":[
                {"context":"global","action":"show-help","keys":["F8"]},
                {"context":"global","action":"show-notifications","keys":["F8"]}
            ]}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(conflict.contains("conflict"));

        let reserved = Keymap::from_json(
            r#"{"bindings":[
                {"context":"global","action":"show-notifications","keys":["Ctrl+S"]}
            ]}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(reserved.contains("reserved"));

        let protocol_leader = Keymap::from_json(r#"{"leader":"Ctrl+?"}"#)
            .unwrap_err()
            .to_string();
        assert!(protocol_leader.contains("depends on enhanced terminal key reporting"));

        let required = Keymap::from_json(
            r#"{"bindings":[
                {"context":"question-options","action":"activate","unbind":true}
            ]}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(required.contains("required action"));

        let printable_prefix = Keymap::from_json(
            r#"{"bindings":[
                {"context":"global","action":"show-help","keys":["g n"]}
            ]}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(printable_prefix.contains("starts with a printable key"));
        assert!(printable_prefix.contains("ordinary Chat text"));

        let shifted_printable_prefix = Keymap::from_json(
            r#"{"bindings":[
                {"context":"global","action":"show-help","keys":["Shift+g n"]}
            ]}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(shifted_printable_prefix.contains("starts with a printable key"));

        for (context, action) in [
            ("chat", "insert-newline"),
            ("session-delete-confirm", "confirm"),
        ] {
            for sequence in ["Ctrl+C x", "F2 Ctrl+C"] {
                let input = format!(
                    r#"{{"bindings":[{{"context":"{context}","action":"{action}","keys":["{sequence}"]}}]}}"#
                );
                let error = Keymap::from_json(&input).unwrap_err().to_string();
                assert!(error.contains("contains single-stroke global quit"));
                assert!(error.contains("always preempts"));
            }
        }

        let pending_escape = Keymap::from_json(
            r#"{"bindings":[
                {"context":"session-delete-confirm","action":"confirm","keys":["F2 Esc"]}
            ]}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(pending_escape.contains("contains Esc after the first stroke"));
        assert!(pending_escape.contains("always cancels a pending sequence"));

        for (context, action) in [
            ("question-number", "activate"),
            ("question-number", "cancel"),
            ("question-inspect", "cancel"),
        ] {
            let input = format!(
                r#"{{"bindings":[{{"context":"{context}","action":"{action}","unbind":true}}]}}"#
            );
            let error = Keymap::from_json(&input).unwrap_err().to_string();
            assert!(
                error.contains("required action"),
                "{context}/{action}: {error}"
            );
        }
    }

    #[test]
    fn unsupported_modifiers_do_not_match_actions_or_text() {
        let keymap = Keymap::default();
        for modifier in [KeyModifiers::SUPER, KeyModifiers::HYPER, KeyModifiers::META] {
            let event = key(KeyCode::Char('y'), modifier);
            assert_eq!(
                keymap.resolve(
                    &[ActionContext::SessionDeleteConfirm, ActionContext::Global,],
                    &mut None,
                    event,
                    Instant::now(),
                ),
                KeyResolution::NoMatch,
                "{modifier:?}+y must not confirm"
            );
            assert_eq!(text_character(event), None);
            assert!(!text_widget_input_allowed(event));
        }
    }

    #[test]
    fn invalid_map_load_falls_back_without_losing_quit_or_help() {
        let temp = std::env::temp_dir().join(format!(
            "bamboo-tui-keymap-{}-{}.json",
            std::process::id(),
            UtcLikeCounter::next()
        ));
        std::fs::write(&temp, "{not json").unwrap();
        let (keymap, warning) = Keymap::load(Some(&temp));
        std::fs::remove_file(&temp).unwrap();
        assert!(warning.unwrap().contains("using conflict-safe defaults"));
        assert_ne!(
            keymap.hint(ActionContext::Global, ActionId::QuitOrStop),
            "unbound"
        );
        assert_ne!(
            keymap.hint(ActionContext::Global, ActionId::ShowHelp),
            "unbound"
        );
    }

    struct UtcLikeCounter;

    impl UtcLikeCounter {
        fn next() -> u64 {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(1);
            NEXT.fetch_add(1, Ordering::Relaxed)
        }
    }

    #[test]
    fn shift_enter_requires_a_terminal_independent_fallback() {
        let error = Keymap::from_json(
            r#"{"bindings":[
                {"context":"chat","action":"insert-newline","keys":["Shift+Enter"]}
            ]}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("terminal-protocol-dependent"));
    }

    #[test]
    fn help_is_generated_from_resolved_custom_bindings() {
        let keymap = Keymap::from_json(
            r#"{"bindings":[
                {"context":"sessions","action":"delete-selection","keys":["F9"]}
            ]}"#,
        )
        .unwrap();
        let entries = keymap.help_entries();
        assert!(entries
            .iter()
            .any(|entry| { entry.keys == "F9" && entry.description.contains("Delete selection") }));
        assert!(!entries.iter().any(|entry| {
            entry.keys == "d" && entry.description.contains("Sessions · Delete selection")
        }));
    }
}
