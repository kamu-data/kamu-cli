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
