use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;

use syn::punctuated::Punctuated;

#[allow(unused)]
fn err(span: Span, message: &str) -> TokenStream {
    syn::Error::new(span, message).into_compile_error().into()
}

#[proc_macro_attribute]
pub fn into_dfut(_args: TokenStream, item: TokenStream) -> TokenStream {
    let exported: syn::ItemImpl = syn::parse(item.clone()).unwrap();

    let worker_ty = exported.self_ty.clone();

    let short_worker_ty = match *exported.self_ty {
        syn::Type::Path(ref type_path) => type_path.path.segments.last().unwrap().ident.clone(),
        _ => unimplemented!(),
    };

    let server_ident = syn::Ident::new(&format!("{short_worker_ty}Server"), Span::call_site());
    let root_client_ident =
        syn::Ident::new(&format!("{short_worker_ty}RootClient"), Span::call_site());
    let client_ident = syn::Ident::new(&format!("{short_worker_ty}Client"), Span::call_site());
    let work_enum_ident = syn::Ident::new(&format!("Work{short_worker_ty}"), Span::call_site());

    let mut client_fns = Vec::new();
    let mut work_variants = Vec::new();
    let mut work_cases = Vec::new();

    let mut handle_cases = Vec::new();
    let mut worker_stub_fns = Vec::new();
    let mut worker_impl_fns = Vec::new();

    let mut fn_names = Vec::new();

    for item in exported.items.clone() {
        match item {
            syn::ImplItem::Fn(f) => {
                let name = f.sig.ident;
                let inputs = f.sig.inputs;
                let output = f.sig.output;
                let block = f.block;

                fn_names.push(name.to_string());

                let mut immutable_inputs = Punctuated::<syn::FnArg, syn::token::Comma>::new();
                for mut input in inputs.clone() {
                    if let syn::FnArg::Typed(ref mut pat_type) = input {
                        if let syn::Pat::Ident(pat_ident) = pat_type.pat.as_mut() {
                            pat_ident.mutability = None;
                        }
                    }
                    immutable_inputs.push(input);
                }

                let d_fut_output = match &output {
                    syn::ReturnType::Default => quote! { dfut::DFut<()> },
                    syn::ReturnType::Type(_, ty) => {
                        let syn::Type::Path(ty) = *ty.clone() else {
                            panic!()
                        };
                        assert_eq!(
                            ty.path.segments.first().unwrap().ident.to_string(),
                            "DResult"
                        );
                        let last_segment = ty.path.segments.last().unwrap();
                        let syn::PathArguments::AngleBracketed(generics) = &last_segment.arguments
                        else {
                            panic!()
                        };
                        let syn::GenericArgument::Type(ty) = &generics.args[0] else {
                            panic!()
                        };
                        quote! { dfut::DResult<dfut::DFut<#ty>> }
                    }
                };

                let mut fn_arg_idents = Vec::new();
                let mut fn_arg_size = Vec::new();

                let mut named = Punctuated::new();
                for input in inputs.clone() {
                    match input {
                        syn::FnArg::Receiver(_) => {}
                        syn::FnArg::Typed(pat_type) => {
                            let ident = match *pat_type.pat {
                                syn::Pat::Ident(pat_ident) => pat_ident.ident,
                                _ => panic!(),
                            };

                            fn_arg_idents.push(ident.clone());
                            fn_arg_size.push(quote! {
                                dfut::size_ser::to_size(&#ident).unwrap()
                            });

                            // TODO: Construct type for each?
                            named.push(syn::Field {
                                attrs: pat_type.attrs,
                                vis: syn::Visibility::Inherited,
                                mutability: syn::FieldMutability::None,
                                ident: Some(ident),
                                colon_token: None,
                                ty: *pat_type.ty,
                            });
                        }
                    }
                }

                let work_variant_ident = syn::Ident::new(
                    &name
                        .to_string()
                        .split("_")
                        .map(|v| v[0..1].to_uppercase() + &v[1..])
                        .collect::<String>(),
                    Span::call_site(),
                );

                work_variants.push(syn::Variant {
                    attrs: Vec::new(),
                    ident: work_variant_ident.clone(),
                    fields: syn::Fields::Named(syn::FieldsNamed {
                        brace_token: syn::token::Brace::default(),
                        named,
                    }),
                    discriminant: None,
                });

                work_cases.push(quote! {
                    #work_enum_ident::#work_variant_ident { .. } => stringify!(#name).to_string(),
                });

                client_fns.push(quote! {
                    pub async fn #name(#immutable_inputs) -> #d_fut_output {
                        self.runtime.do_remote_work(#work_enum_ident::#work_variant_ident {
                            #(#fn_arg_idents),*
                        }).await
                    }
                });

                let fn_impl_name = syn::Ident::new(&format!("___{name}"), Span::call_site());

                // TODO: we can make lineage reconstruction a compile time choice.
                //
                // It requires keeping a clone of the inputs until the dfuture is freed
                // so that the computation can be restarted if any child computations fail.
                //
                // TODO: can we make worker pinning a compile time choice?
                worker_stub_fns.push(quote! {
                    pub async fn #name(#immutable_inputs) -> #d_fut_output {
                        let fn_name = stringify!(#name);

                        let size = #(#fn_arg_size)+*;

                        match self.runtime.schedule_work(fn_name, size)? {
                            dfut::Where::Local => {
                                Ok(self.runtime.do_local_work_fut(
                                    fn_name,
                                    |r| async move {
                                        #worker_ty::new(r).#fn_impl_name(#(#fn_arg_idents),*).await
                                    }
                                ))
                            },
                            dfut::Where::Remote { address } => {
                                self.runtime.do_remote_work(&address, #work_enum_ident::#work_variant_ident {
                                    #(#fn_arg_idents),*
                                }).await
                            }
                        }
                    }
                });

                // TODO: We can optionally use boxed future depending on if the user specified that
                // a d_fn is recursive.
                //
                // We probably want worker_impl_fns to just be `pub async fn`
                // rather than fn(inputs) -> BoxedFuture<Output>. But
                let worker_impl_output = match &output {
                    syn::ReturnType::Default => {
                        quote! { std::pin::Pin<Box<dyn std::future::Future<Output=()> + Send + '_>> }
                    }
                    syn::ReturnType::Type(_, ty) => {
                        quote! { std::pin::Pin<Box<dyn std::future::Future<Output=#ty> + Send + '_>> }
                    }
                };

                worker_impl_fns.push(quote! {
                    pub fn #fn_impl_name(#inputs) -> #worker_impl_output {
                        Box::pin(async move #block)
                    }
                });

                handle_cases.push(quote! {
                    #work_enum_ident::#work_variant_ident {
                        #(#fn_arg_idents),*
                    } => {
                        let fn_name = stringify!(#name);
                        self.root_runtime.do_local_work(
                            parent_info,
                            fn_name,
                            |runtime| async move {
                                #worker_ty::new(runtime)
                                    .#fn_impl_name(#(#fn_arg_idents),*)
                                    .await
                            }
                        )?
                    }
                });
            }
            _ => unimplemented!(),
        }
    }

    let expaned = quote! {
        #[derive(serde::Serialize, serde::Deserialize)]
        enum #work_enum_ident {
            #(#work_variants),*
        }

        impl dfut::IntoWork for #work_enum_ident {
            fn into_work(&self) -> dfut::Work {
                let fn_name = match self {
                    #(#work_cases)*
                };
                let args = dfut::bincode::serialize(self).unwrap();
                dfut::Work {
                    fn_name,
                    args,
                }
            }
        }

        pub struct #root_client_ident {
            root_runtime: dfut::RootRuntimeClient,
        }

        impl #root_client_ident {
           pub async fn new(global_scheduler_address: &str, unique_client_id: &str) -> Self {
               let root_runtime = dfut::RootRuntimeClient::new(
                   global_scheduler_address,
                   unique_client_id
               ).await;
               Self { root_runtime }
           }

           pub fn new_client(&self) -> #client_ident {
               #client_ident {
                   runtime: self.root_runtime.new_runtime_client(),
               }
           }
        }

        pub struct #client_ident {
            runtime: dfut::RuntimeClient,
        }

        impl #client_ident {
            pub async fn d_await<T>(&self, t: dfut::DFut<T>) -> dfut::DResult<T>
            where
                T: dfut::Serialize + dfut::DeserializeOwned + std::fmt::Debug + Clone + Send + Sync + 'static,
            {
                self.runtime.wait(t).await
            }

            pub async fn d_cancel<T>(&self, d_fut: DFut<T>)
            where
                T: dfut::DeserializeOwned
            {
                self.runtime.cancel(d_fut).await.unwrap()
            }
        }

        /// Stub functions that run on the cluster.
        impl #client_ident {
            #(#client_fns)*
        }

        impl #worker_ty {
            pub fn new(runtime: dfut::Runtime) -> Self {
                Self {
                    runtime,
                }
            }

            pub async fn serve(worker_server_config: dfut::WorkerServerConfig) -> dfut::RootRuntimeHandle {
                let available_fn_names = vec![#(#fn_names.to_string()),*];
                dfut::RootRuntime::serve::<#server_ident>(
                    worker_server_config,
                    available_fn_names,
                ).await
            }

            pub async fn d_await<T>(&self, d_fut: dfut::DFut<T>) -> dfut::DResult<T>
            where
                T: dfut::Serialize + dfut::DeserializeOwned + std::fmt::Debug + Clone + Send + Sync + 'static,
            {
                self.runtime.wait(d_fut).await
            }

            pub async fn d_cancel<T>(&self, d_fut: dfut::DFut<T>) -> dfut::DResult<()>
            where
                T: dfut::DeserializeOwned
            {
                self.runtime.cancel(d_fut).await
            }

            pub async fn d_box<T>(&self, t: T) -> dfut::DResult<dfut::DFut<T>>
            where
                T: dfut::Serialize + std::fmt::Debug + Send + Sync + 'static
            {
                self.runtime.d_box(t).await
            }
        }

        /// Generated stub functions that either execute work remotely or locally.
        impl #worker_ty {
            #(#worker_stub_fns)*
        }

        /// Function impls are moved here.
        impl #worker_ty {
            #(#worker_impl_fns)*
        }


        struct #server_ident {
            root_runtime: dfut::RootRuntime,
        }

        #[dfut::tonic::async_trait]
        impl dfut::WorkerService for #server_ident {
            async fn do_work(
                &self,
                request: dfut::tonic::Request<dfut::DoWorkRequest>,
            ) -> Result<dfut::tonic::Response<dfut::DoWorkResponse>, dfut::tonic::Status> {
                let dfut::DoWorkRequest { parent_info, fn_name, args, .. } = request.into_inner();

                let args: #work_enum_ident = dfut::bincode::deserialize(&args).unwrap();
                let resp = match args {
                    #(#handle_cases),*
                };

                Ok(dfut::tonic::Response::new(resp))
            }
        }

        impl dfut::WorkerServiceExt for #server_ident {
            fn new(root_runtime: dfut::RootRuntime) -> Self {
                Self {
                    root_runtime,
                }
            }
        }
    };

    TokenStream::from(expaned)
}

#[proc_macro]
pub fn d_await(item: TokenStream) -> TokenStream {
    let e: syn::Expr = syn::parse(item).unwrap();
    let expanded = quote! { self.d_await(#e).await? };
    TokenStream::from(expanded)
}

#[proc_macro]
pub fn d_cancel(item: TokenStream) -> TokenStream {
    let e: syn::Expr = syn::parse(item).unwrap();
    let expanded = quote! { self.d_cancel(#e).await? };
    TokenStream::from(expanded)
}

#[proc_macro]
pub fn d_box(item: TokenStream) -> TokenStream {
    let e: syn::Expr = syn::parse(item).unwrap();
    let expanded = quote! { self.d_box(#e).await? };
    TokenStream::from(expanded)
}
