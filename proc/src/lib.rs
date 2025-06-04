use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream, Result};
use syn::punctuated::Punctuated;
use syn::{braced, parse_macro_input, Ident, Token};

struct CrdtTupleInput {
    name: Ident,
    fields: Punctuated<(Ident, Ident), Token![,]>,
}

impl Parse for CrdtTupleInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let name: Ident = input.parse()?;
        let content;
        braced!(content in input);
        let fields = content.parse_terminated(
            |input| {
                let name: Ident = input.parse()?;
                input.parse::<Token![:]>()?;
                let ty: Ident = input.parse()?;
                Ok((name, ty))
            },
            Token![,],
        )?;
        Ok(CrdtTupleInput { name, fields })
    }
}

#[proc_macro]
pub fn crdt_tuple(input: TokenStream) -> TokenStream {
    let CrdtTupleInput { name, fields } = parse_macro_input!(input as CrdtTupleInput);

    let variant_idents: Vec<Ident> = fields
        .iter()
        .map(|(field, _)| format_ident!("{}", field.to_string().to_uppercase()))
        .collect();
    let field_idents: Vec<Ident> = fields.iter().map(|(field, _)| field.clone()).collect();
    let field_types: Vec<Ident> = fields.iter().map(|(_, ty)| ty.clone()).collect();

    let op_enum = quote! {
        #[derive(Clone, Debug)]
        pub enum #name Op<#(#field_types),*> {
            #(
                #variant_idents(#field_types),
            )*
        }
    };

    let log_struct = quote! {
        #[derive(Clone, Debug, Default)]
        pub struct #name Log<#(#field_types),*> {
            #(
                #field_idents: #field_types,
            )*
        }
    };

    let log_impl = quote! {
        impl<#(#field_types),*> Log for #name Log<#(#field_types),*>
        where
            #(#field_types: Log,)*
        {
            type Value = (#(#field_types::Value),*);
            type Op = #name Op<#(#field_types),*>;

            fn new() -> Self {
                Self {
                    #(
                        #field_idents: #field_types::new(),
                    )*
                }
            }

            fn new_event(&mut self, event: &Event<Self::Op>) {
                match &event.op {
                    #(
                        #name Op::#variant_idents(ref op) => {
                            let evt = Event::new(op.clone(), event.metadata().clone());
                            self.#field_idents.new_event(&evt);
                        },
                    )*
                }
            }

            fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool) {
                match &event.op {
                    #(
                        #name Op::#variant_idents(ref op) => {
                            let evt = Event::new(op.clone(), event.metadata().clone());
                            self.#field_idents.prune_redundant_events(&evt, is_r_0);
                        },
                    )*
                }
            }

            fn purge_stable_metadata(&mut self, metadata: &Clock<Partial>) {
                #(
                    self.#field_idents.purge_stable_metadata(metadata);
                )*
            }

            fn collect_events(&self, upper: &Clock<Full>, lower: &Clock<Full>) -> Vec<Event<Self::Op>> {
                let mut events = vec![];
                #(
                    events.extend(self.#field_idents.collect_events(upper, lower).into_iter().map(|e| Event::new(#name Op::#variant_idents(e.op.clone()), e.metadata().clone())));
                )*
                events
            }

            fn collect_events_since(&self, since: &Since) -> Vec<Event<Self::Op>> {
                let mut events = vec![];
                #(
                    events.extend(self.#field_idents.collect_events_since(since).into_iter().map(|e| Event::new(#name Op::#variant_idents(e.op.clone()), e.metadata().clone())));
                )*
                events
            }

            fn r_n(&mut self, metadata: &Clock<Full>, conservative: bool) {
                #(
                    self.#field_idents.r_n(metadata, conservative);
                )*
            }

            fn any_r(&self, event: &Event<Self::Op>) -> bool {
                match &event.op {
                    #(
                        #name Op::#variant_idents(ref op) => {
                            let evt = Event::new(op.clone(), event.metadata().clone());
                            self.#field_idents.any_r(&evt)
                        },
                    )*
                }
            }

            fn eval(&self) -> Self::Value {
                (#(self.#field_idents.eval()),*)
            }

            fn stabilize(&mut self, metadata: &Clock<Partial>) {
                #(
                    self.#field_idents.stabilize(metadata);
                )*
            }

            fn is_empty(&self) -> bool {
                true #(&& self.#field_idents.is_empty())*
            }

            fn size(&self) -> usize {
                0 #( + self.#field_idents.size() )*
            }

            fn deps(&self, clocks: &mut VecDeque<Clock<Partial>>, view: &Rc<ViewData>, dot: &Dot, op: &Self::Op) {
                match op {
                    #(
                        #name Op::#variant_idents(ref op) => self.#field_idents.deps(clocks, view, dot, op),
                    )*
                }
            }
        }
    };

    quote! {
        #op_enum
        #log_struct
        #log_impl
    }
    .into()
}
