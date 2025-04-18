use alloy_primitives::{Address, Bytes};
use parking_lot::RwLock;
use reth_hyperliquid_types::{ReadPrecompileInput, ReadPrecompileResult};
use reth_revm::{
    context::{Cfg, ContextTr},
    handler::{EthPrecompiles, PrecompileProvider},
    interpreter::{Gas, InstructionResult, InterpreterResult},
    precompile::{PrecompileError, PrecompileErrors},
};
use std::{collections::HashMap, sync::Arc};

/// Precompile that replays cached results.
#[derive(Clone)]
pub struct ReplayPrecompile<CTX: ContextTr> {
    precompiles: EthPrecompiles<CTX>,
    cache: Arc<RwLock<HashMap<Address, HashMap<ReadPrecompileInput, ReadPrecompileResult>>>>,
}

impl<CTX: ContextTr> std::fmt::Debug for ReplayPrecompile<CTX> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayPrecompile").finish()
    }
}

impl<CTX: ContextTr> ReplayPrecompile<CTX> {
    /// Creates a new replay precompile with the given precompiles and cache.
    pub fn new(
        precompiles: EthPrecompiles<CTX>,
        cache: Arc<RwLock<HashMap<Address, HashMap<ReadPrecompileInput, ReadPrecompileResult>>>>,
    ) -> Self {
        Self { precompiles, cache }
    }
}

impl<CTX: ContextTr> PrecompileProvider for ReplayPrecompile<CTX> {
    type Context = CTX;
    type Output = InterpreterResult;

    fn set_spec(&mut self, spec: <<Self::Context as ContextTr>::Cfg as Cfg>::Spec) {
        self.precompiles.set_spec(spec);
    }

    fn run(
        &mut self,
        context: &mut Self::Context,
        address: &Address,
        bytes: &Bytes,
        gas_limit: u64,
    ) -> Result<Option<Self::Output>, PrecompileErrors> {
        let cache = self.cache.read();
        if let Some(precompile_calls) = cache.get(address) {
            let input = ReadPrecompileInput { input: bytes.clone(), gas_limit };
            let mut result = InterpreterResult {
                result: InstructionResult::Return,
                gas: Gas::new(gas_limit),
                output: Bytes::new(),
            };

            return match *precompile_calls.get(&input).expect("missing precompile call") {
                ReadPrecompileResult::Ok { gas_used, ref bytes } => {
                    let underflow = result.gas.record_cost(gas_used);
                    assert!(underflow, "Gas underflow is not possible");
                    result.output = bytes.clone();
                    Ok(Some(result))
                }
                ReadPrecompileResult::OutOfGas => Err(PrecompileError::OutOfGas.into()),
                ReadPrecompileResult::Error => {
                    Err(PrecompileError::other("precompile failed").into())
                }
                ReadPrecompileResult::UnexpectedError => panic!("unexpected precompile error"),
            };
        }

        // If no cached result, fall back to normal precompile execution
        self.precompiles.run(context, address, bytes, gas_limit)
    }

    fn contains(&self, address: &Address) -> bool {
        self.precompiles.contains(address) || self.cache.read().get(address).is_some()
    }

    fn warm_addresses(&self) -> Box<impl Iterator<Item = Address> + '_> {
        let addresses: Vec<Address> =
            self.precompiles.warm_addresses().chain(self.cache.read().keys().cloned()).collect();
        Box::new(addresses.into_iter())
    }
}
