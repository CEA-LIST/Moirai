/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//  
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
// 
//  SPDX-License-Identifier: Apache-2.0
//  
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

pub mod bag;
pub mod counter;
pub mod flag;
pub mod graph;
pub mod json;
pub mod list;
pub mod map;
pub mod model;
pub mod option;
pub mod query;
pub mod register;
pub mod set;
pub mod utils;

type FxHashMap<K, V> = rustc_hash::FxHashMap<K, V>;
type FxHashSet<V> = rustc_hash::FxHashSet<V>;
