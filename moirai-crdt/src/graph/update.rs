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

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
pub use moirai_macros::typed_graph::update::{TypedGraphUpdate, VertexUpdate};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum GraphUpdate<V, E> {
    /// Add a vertex with identifier `V`. `V` must be unique.
    AddVertex(V),
    /// Remove a vertex with identifier `V`. All arcs connected to this vertex are also removed.
    RemoveVertex(V),
    /// Add an arc from vertex `V` to vertex `V'` with edge identifier `E`. Both vertices must already exist. The triple `(V, V', E)` must be unique.
    AddArc(V, V, E),
    /// Remove an arc from vertex `V` to vertex `V'` with edge identifier `E`. The triple `(V, V', E)` must already exist.
    RemoveArc(V, V, E),
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum LabelledMultidigraphUpdate<V, E, No, Lo> {
    UpdateVertex {
        id: V,
        child: No,
    },
    RemoveVertex {
        id: V,
    },
    UpdateArc {
        source: V,
        target: V,
        id: E,
        child: Lo,
    },
    RemoveArc {
        source: V,
        target: V,
        id: E,
    },
}
