use std::sync::Mutex;

use kamu::domain::PullImageListener;
use read_input::prelude::*;

pub fn prompt_yes_no(msg: &str) -> bool {
    let answer: String = input()
        .repeat_msg(msg)
        .default("n".to_owned())
        .add_test(|v| match v.as_ref() {
            "n" | "N" | "no" | "y" | "Y" | "yes" => true,
            _ => false,
        })
        .get();

    match answer.as_ref() {
        "n" | "N" | "no" => false,
        _ => true,
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PullImageProgress {
    image_purpose: &'static str,
    progress_bar: Mutex<Option<indicatif::ProgressBar>>,
}

impl PullImageProgress {
    pub fn new(image_purpose: &'static str) -> Self {
        Self {
            image_purpose,
            progress_bar: Mutex::new(None),
        }
    }
}

impl PullImageListener for PullImageProgress {
    fn begin(&self, image: &str) {
        let s = indicatif::ProgressBar::new_spinner();
        s.set_style(indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"));
        s.set_message(format!("Pulling {} image {}", self.image_purpose, image));
        s.enable_steady_tick(100);
        self.progress_bar.lock().unwrap().replace(s);
    }

    fn success(&self) {
        self.progress_bar.lock().unwrap().take();
    }
}
