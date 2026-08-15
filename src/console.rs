use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver, Sender};
use bevy::input::keyboard::{Key, KeyboardInput};
use bevy::input::ButtonState;
use bevy::log::tracing::{self, Level, Subscriber};
use bevy::log::tracing_subscriber::{layer::Context, Layer};
use bevy::log::{BoxedLayer, LogPlugin};
use bevy::prelude::*;

#[derive(Resource, Default)]
struct ConsoleCommands {
    commands: Vec<ConsoleCommand>,
}


impl ConsoleCommands {
    pub fn register(&mut self, name: impl Into<String>, handler: fn(&mut DevConsole, &[&str])) {
        let name = name.into();

        if let Some(existing) = self.commands.iter_mut().find(|c| c.name == name) {
            existing.handler = handler;
        } else {
            self.commands.push(ConsoleCommand { name, handler });
        }
    }
    pub fn unregister(&mut self, name: &str) -> bool {
        let len_before = self.commands.len();
        self.commands.retain(|c| c.name != name);
        self.commands.len() != len_before
    }
}

#[derive(Clone)]
struct ConsoleCommand {
    name: String,
    handler: fn(&mut DevConsole, &[&str]),
}

pub struct ConsolePlugin {
    commands: Vec<ConsoleCommand>,
}

impl Default for ConsolePlugin {
    fn default() -> Self {
        Self {
            commands: Vec::new(),
        }
    }
}

impl Plugin for ConsolePlugin {
    fn build(&self, app: &mut App) {
        app.insert_resource(ConsoleCommands {
            commands: self.commands.clone(),
        })
        .init_resource::<DevConsole>()
        .init_resource::<BevyLogFeed>()
        .add_systems(Startup, (setup_log_backdrop, setup_console_ui))
        .add_systems(
            Update,
            (
                (collect_bevy_logs, refresh_log_backdrop).chain(),
                (toggle_console, console_input, refresh_console_display).chain(),
            ),
        );
    }
}

impl ConsolePlugin {
    pub fn command(
        mut self,
        name: impl Into<String>,
        handler: fn(&mut DevConsole, &[&str]),
    ) -> Self {
        self.commands.push(ConsoleCommand {
            name: name.into(),
            handler,
        });

        self
    }
    pub fn log_plugin() -> LogPlugin {
        LogPlugin {
            custom_layer: install_log_capture,
            ..default()
        }
    }
}

#[derive(Message, Clone)]
struct CapturedLog {
    level: Level,
    target: String,
    message: String,
}

struct CaptureLayer {
    sender: Sender<CapturedLog>,
}

struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(
        &mut self,
        field: &tracing::field::Field,
        value: &dyn std::fmt::Debug,
    ) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: Context<'_, S>,
    ) {
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);

        let _ = self.sender.send(CapturedLog {
            level: *event.metadata().level(),
            target: event.metadata().target().to_string(),
            message: visitor.0,
        });
    }
}

struct CapturedLogReceiver(Receiver<CapturedLog>);

fn install_log_capture(app: &mut App) -> Option<BoxedLayer> {
    let (sender, receiver) = mpsc::channel();

    app.insert_non_send(CapturedLogReceiver(receiver))
        .add_message::<CapturedLog>()
        .add_systems(Update, transfer_captured_logs);

    Some(CaptureLayer { sender }.boxed())
}

fn transfer_captured_logs(
    receiver: NonSend<CapturedLogReceiver>,
    mut writer: MessageWriter<CapturedLog>,
) {
    while let Ok(log) = receiver.0.try_recv() {
        writer.write(log);
    }
}

const LOG_FEED_CAP: usize = 300;
const LOG_FEED_VISIBLE: usize = 24;

#[derive(Resource, Default)]
struct BevyLogFeed {
    lines: VecDeque<String>,
}

fn collect_bevy_logs(
    mut reader: MessageReader<CapturedLog>,
    mut feed: ResMut<BevyLogFeed>,
) {
    for log in reader.read() {
        feed.lines.push_back(format!(
            "[{}] {}: {}",
            log.level,
            log.target,
            log.message
        ));

        if feed.lines.len() > LOG_FEED_CAP {
            feed.lines.pop_front();
        }
    }
}

#[derive(Component)]
struct BevyLogBackdropText;

fn setup_log_backdrop(mut commands: Commands) {
    commands.spawn((
        BevyLogBackdropText,
        Text::new(""),
        TextFont {
            font_size: FontSize::Rem(1.2),
            ..default()
        },
        TextColor(Color::srgba(0.4, 0.9, 0.5, 0.18)),
        Node {
            position_type: PositionType::Absolute,
            left: Val::Px(8.0),
            top: Val::Px(8.0),
            right: Val::Px(8.0),
            bottom: Val::Px(8.0),
            overflow: Overflow::clip(),
            ..default()
        },
    ));
}

fn refresh_log_backdrop(
    feed: Res<BevyLogFeed>,
    mut text_q: Query<
        &mut Text,
        With<BevyLogBackdropText>,
    >,
) {
    if !feed.is_changed() {
        return;
    }

    if let Ok(mut text) = text_q.single_mut() {
        let tail: Vec<String> = feed
            .lines
            .iter()
            .rev()
            .take(LOG_FEED_VISIBLE)
            .rev()
            .cloned()
            .collect();

        text.0 = tail.join("\n");
    }
}

#[derive(Resource)]
pub struct DevConsole {
    open: bool,
    input: String,
    pub(crate) log: Vec<String>,
}

impl Default for DevConsole {
    fn default() -> Self {
        Self {
            open: false,
            input: String::new(),
            log: vec![
                "dev console — type `help` and press Enter".to_string()
            ],
        }
    }
}

#[derive(Component)]
struct ConsoleRoot;

#[derive(Component)]
struct ConsoleLogText;

#[derive(Component)]
struct ConsoleInputText;

fn setup_console_ui(mut commands: Commands) {
    commands
        .spawn((
            ConsoleRoot,
            Node {
                position_type: PositionType::Absolute,
                left: Val::Px(0.0),
                top: Val::Px(0.0),
                width: Val::Percent(100.0),
                height: Val::Auto,
                max_height: Val::Percent(60.0),
                flex_direction: FlexDirection::Column,
                padding: UiRect::all(Val::Px(8.0)),
                overflow: Overflow::clip(),
                ..default()
            },
            BackgroundColor(Color::srgba(0.0, 0.0, 0.0, 0.85)),
            Visibility::Hidden,
        ))
        .with_children(|parent| {
            parent.spawn((
                ConsoleLogText,
                Text::new(""),
                TextFont {
                    font_size: FontSize::Rem(2.0),
                    ..default()
                },
                TextColor(Color::srgb(0.85, 0.85, 0.85)),
            ));

            parent.spawn((
                ConsoleInputText,
                Text::new("> "),
                TextFont {
                    font_size: FontSize::Rem(2.0),
                    ..default()
                },
                TextColor(Color::srgb(0.2, 1.0, 0.4)),
            ));
        });
}

fn toggle_console(
    keys: Res<ButtonInput<KeyCode>>,
    mut console: ResMut<DevConsole>,
    mut root_q: Query<&mut Visibility, With<ConsoleRoot>>,
) {
    if keys.just_pressed(KeyCode::Backquote) {
        console.open = !console.open;

        if let Ok(mut vis) = root_q.single_mut() {
            *vis = if console.open {
                Visibility::Visible
            } else {
                Visibility::Hidden
            };
        }
    }
}

fn console_input(
    mut console: ResMut<DevConsole>,
    commands: Res<ConsoleCommands>,
    mut kbd_evr: MessageReader<KeyboardInput>,
) {
    if !console.open {
        kbd_evr.clear();
        return;
    }

    for ev in kbd_evr.read() {
        if ev.state == ButtonState::Released {
            continue;
        }

    match &ev.logical_key {
        Key::Enter => {
            let line = console.input.trim().to_string();
            console.input.clear();

            if line.is_empty() {
                continue;
            }

            console.log.push(format!("> {line}"));
            run_command(&line, &mut console, &commands);
        }

        Key::Backspace => {
            console.input.pop();
        }

        Key::Space => {
            console.input.push(' ');
        }

        Key::Character(s) => {
            if s.as_str() == "`" || s.as_str() == "~" {
                continue;
            }

            if s.chars().any(|c| c.is_control()) {
                continue;
            }

            console.input.push_str(s);
        }

        _ => {}
    }
    }
}

fn run_command(
    line: &str,
    console: &mut DevConsole,
    commands: &ConsoleCommands,
) {
    let mut parts = line.split_whitespace();
    let cmd = parts.next().unwrap_or_default();
    let args: Vec<&str> = parts.collect();
    if cmd == "clear" {
        console.log.clear();
        return;
    }
    if cmd == "help" {
        console.log.push(format!(
            "commands: {}, clear",
            commands
                .commands
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        return;
    }
    if let Some(command) = commands.commands.iter().find(|c| c.name == cmd) {
        (command.handler)(console, &args);
    } else {
        console
            .log
            .push(format!("unknown command: {cmd}"));
    }
}

fn refresh_console_display(
    console: Res<DevConsole>,
    mut log_q: Query<
        &mut Text,
        (
            With<ConsoleLogText>,
            Without<ConsoleInputText>,
        ),
    >,
    mut input_q: Query<
        &mut Text,
        (
            With<ConsoleInputText>,
            Without<ConsoleLogText>,
        ),
    >,
) {
    if !console.is_changed() {
        return;
    }

    if let Ok(mut text) = log_q.single_mut() {
        let tail: Vec<String> = console
            .log
            .iter()
            .rev()
            .take(12)
            .rev()
            .cloned()
            .collect();

        text.0 = tail.join("\n");
    }

    if let Ok(mut text) = input_q.single_mut() {
        text.0 = format!("> {}", console.input);
    }
}