use bevy::prelude::*;
mod console;
mod lua;

fn setup_camera(mut commands: Commands) {
    commands.spawn(Camera2d);
}

fn main() {
    App::new()
        .add_systems(Startup, setup_camera)
        .add_plugins(DefaultPlugins.set(console::ConsolePlugin::log_plugin()))
        .add_plugins(lua::LuaScriptPlugin::default())
        .add_plugins(
            console::ConsolePlugin::default()
        )
        .run();
}