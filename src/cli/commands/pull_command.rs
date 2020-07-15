use super::Command;
use indicatif::*;

pub struct PullCommand;

impl PullCommand {
    pub fn new() -> PullCommand {
        PullCommand
    }
}

impl Command for PullCommand {
    fn run(&mut self) {
        let bar = ProgressBar::new(100);
        bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {wide_bar:.cyan/blue} {pos:>5}/{len:>5} {msg}"),
        );
        for _ in 0..100 {
            bar.inc(1);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        bar.finish_with_message("OK");
    }
}
