// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(assert_matches)]

mod tests;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! test_message_type {
    ($message_type_suffix: ident) => {
        paste::paste! {
            #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
            pub(crate) struct [<TestMessage $message_type_suffix>] {
                pub(crate) body: String,
            }

            impl Message for [<TestMessage $message_type_suffix>] {
                fn version() -> u32 {
                    1
                }
            }
        }
    };
}

macro_rules! test_message_consumer {
    ($message_type_suffix: ident, $message_consumer_suffix: ident, $producer_name: ident, $delivery: ident, $initial_consumer_boundary: ident) => {
        paste::paste! {
            struct [<"TestMessageConsumer" $message_consumer_suffix>] {
                state: Arc<Mutex<[<"State" $message_consumer_suffix>]>>,
            }

            struct [<"State" $message_consumer_suffix>] {
                captured_messages: Vec<[<TestMessage $message_type_suffix>]>,
            }

            impl Default for [<"State" $message_consumer_suffix>] {
                fn default() -> Self {
                    Self {
                        captured_messages: vec![],
                    }
                }
            }

            #[component(pub)]
            #[scope(Singleton)]
            #[interface(dyn MessageConsumer)]
            #[interface(dyn MessageConsumerT<[<TestMessage $message_type_suffix>]>)]
            #[meta(MessageConsumerMeta {
                consumer_name: concat!("TestMessageConsumer", stringify!($message_consumer_suffix)),
                feeding_producers: &[$producer_name],
                delivery: MessageDeliveryMechanism::$delivery,
                initial_consumer_boundary: InitialConsumerBoundary::$initial_consumer_boundary,

            })]
            impl [<"TestMessageConsumer" $message_consumer_suffix>] {
                fn new() -> Self {
                    Self {
                        state: Arc::new(Mutex::new(Default::default())),
                    }
                }

                #[allow(dead_code)]
                fn get_messages(&self) -> Vec<[<TestMessage $message_type_suffix>]> {
                    let guard = self.state.lock().unwrap();
                    guard.captured_messages.clone()
                }
            }

            impl MessageConsumer for [<"TestMessageConsumer" $message_consumer_suffix>] {}

            #[async_trait::async_trait]
            impl MessageConsumerT<[<TestMessage $message_type_suffix>]> for [<"TestMessageConsumer" $message_consumer_suffix>] {
                async fn consume_message(
                    &self,
                    _: &Catalog,
                    message: &[<TestMessage $message_type_suffix>],
                ) -> Result<(), InternalError> {
                    let mut guard = self.state.lock().unwrap();
                    guard.captured_messages.push(message.clone());
                    Ok(())
                }
            }
        }
    };
}

pub(crate) use {test_message_consumer, test_message_type};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
