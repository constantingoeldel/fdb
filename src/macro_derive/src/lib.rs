use proc_macro::TokenStream;

use proc_macro2::{Span, TokenStream as TokenStr};
use quote::quote;
use syn::{Field, Fields, Ident};

#[proc_macro_derive(DeserializeUntagged)]
pub fn derive_deserialize_untagged(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_deserialize_untagged(&ast)
}

fn impl_deserialize_untagged(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;

    let variants = match &ast.data {
        syn::Data::Enum(data) => &data.variants,
        _ => panic!("Only enums are supported"),
    };

    let variants = variants.iter().map(|variant| {
        let ident = &variant.ident;
        let fields = &variant.fields;
        let deserializer = match &variant.fields {
            Fields::Unnamed(fields) => fields.unnamed.first().unwrap(),
            _ => panic!("Only unnamed fields are supported"),
        };
        (ident, deserializer)
    }).collect::<Vec<_>>();


    let variant_impls: Vec<TokenStr> = variants.iter().map(|(variant, deserializer)| {
        variant_testing(name, variant, deserializer)
    }).collect();

    let variant_reapplications: Vec<TokenStr> = variants.iter().map(|(variant, deserializer)| {
        variant_reapplication(name, variant, deserializer)
    }).collect();

    let gen = quote! {

        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
            {
                        // See https://stackoverflow.com/questions/75181286/how-to-implement-a-custom-deserializer-using-serde-that-allows-for-parsing-of-un/78793511#78793511
                    fn try_untagged_enum_variants<E>(v: &[u8]) -> Result<#name, E> where E: serde::de::Error {

                        let mut errors = Vec::new();
                        #(#variant_impls)*

                        let errors_str = errors.iter().map(|e| e.to_string()).collect::<Vec<String>>().join(" | ");
                        Err(serde::de::Error::custom(format!("No fitting option found. Errors: {}", errors_str)))
                    }


                let buf = deserializer.get_underlying_buffer();

                let variant = try_untagged_enum_variants(buf)?;

                // Reapply fitting version to correctly trim the buffer
                match variant {
                    #(#variant_reapplications)*
                }

                Ok(variant)

            }
        }
    };
    gen.into()
}

fn variant_testing(enum_name: &Ident, variant: &Ident, deserializer: &Field) -> TokenStr {
    quote! {
            let variant: Result<#deserializer, crate::parser::ParseError> = crate::parser::from_slice(v);
            match variant {
                Ok(res) => {
                return Ok(#enum_name::#variant(res))
                },
                Err(e) => {
                    // dbg!(&e);
                    errors.push(e);
            }
            }
    }
}

fn variant_reapplication(enum_name: &Ident, variant: &Ident, deserializer: &Field) -> TokenStr {
    quote! {
        #enum_name::#variant(_) => { #deserializer::deserialize(deserializer).unwrap(); },
    }
}


#[proc_macro_derive(Options)]
pub fn derive_options(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_options(&ast)
}

fn impl_options(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let options = match &ast.data {
        syn::Data::Struct(data) => &data.fields,
        _ => panic!("Only structs are supported"),
    };

    fn capitalize_first(s: &str) -> String {
        let mut c = s.chars();
        match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        }
    }

    fn extract_type_from_option(ty: &syn::Type) -> Option<&syn::Type> {
        use syn::{GenericArgument, Path, PathArguments, PathSegment};

        fn extract_type_path(ty: &syn::Type) -> Option<&Path> {
            match *ty {
                syn::Type::Path(ref typepath) if typepath.qself.is_none() => Some(&typepath.path),
                _ => None,
            }
        }
        // https://stackoverflow.com/questions/55271857/how-can-i-get-the-t-from-an-optiont-when-using-syn
        // TODO store (with lazy static) the vec of string
        // TODO maybe optimization, reverse the order of segments
        fn extract_option_segment(path: &Path) -> Option<&PathSegment> {
            let idents_of_path = path
                .segments
                .iter()
                .into_iter()
                .fold(String::new(), |mut acc, v| {
                    acc.push_str(&v.ident.to_string());
                    acc.push('|');
                    acc
                });
            vec!["Option|", "std|option|Option|", "core|option|Option|"]
                .into_iter()
                .find(|s| &idents_of_path == *s)
                .and_then(|_| path.segments.last())
        }

        extract_type_path(ty)
            .and_then(|path| extract_option_segment(path))
            .and_then(|path_seg| {
                let type_params = &path_seg.arguments;
                // It should have only on angle-bracketed param ("<String>"):
                match *type_params {
                    PathArguments::AngleBracketed(ref params) => params.args.first(),
                    _ => None,
                }
            })
            .and_then(|generic_arg| match *generic_arg {
                GenericArgument::Type(ref ty) => Some(ty),
                _ => None,
            })
    }

    fn options_enum_variant(field: &syn::Field) -> TokenStr {
        let field_name = Ident::new(&capitalize_first(&field.ident.as_ref().unwrap().to_string()), Span::call_site());
        let field_type = &field.ty; // Field type is wrapped in an option, wee need to unwrap it Option<T> -> T
        let inner = extract_type_from_option(field_type).expect("Field type is not an Option");
        quote! {
            #field_name(#inner),
        }
    }

    let options_enum_variants: Vec<TokenStr> = options.iter().map(options_enum_variant).collect();


    fn options_enum_match(field: &Field) -> TokenStr {
        let field_name = &field.ident.as_ref().unwrap();
        let field_name_cap = Ident::new(&capitalize_first(&field_name.to_string()), Span::call_site());

        quote! {
            OptionsEnum::#field_name_cap(#field_name) => {
                if options.#field_name.is_some() {
                    return Err(serde::de::Error::duplicate_field(stringify!(#field_name)));
                }
                options.#field_name = Some(#field_name);
            },
        }
    }

    let options_enum_matches: Vec<TokenStr> = options.iter().map(options_enum_match).collect();


    let gen = quote! {
        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_seq(OptionsVisitor)
            }
        }

        #[derive(DeserializeUntagged, Debug, Eq, PartialEq)]
        enum OptionsEnum {
            #(#options_enum_variants)*
        }

        struct OptionsVisitor;

        impl<'de> serde::de::Visitor<'de> for OptionsVisitor {
            type Value = #name;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("One or more variants of the enum")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error> where A: serde::de::SeqAccess<'de> {
                let mut options = #name::default();

                while let Ok(Some(option)) = seq.next_element() {

                    match option {
                        #(#options_enum_matches)*
                    }
                }

                Ok(options)
            }
        }
    };
    gen.into()
}
//
//
// impl<'de> Deserialize<'de> for Options {
//     fn deserialize<D>(mut deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
//         #[derive(DeserializeUntagged, Debug, Eq, PartialEq)]
//         enum OptionsEnum {
//             Expiry(Expiry),
//             Existence(Existence),
//             GET(GET),
//         }
//
//         struct OptionsVisitor;
//
//         impl<'de> Visitor<'de> for OptionsVisitor {
//             type Value = Options;
//
//             fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
//                 formatter.write_str("One or more variants of the enum")
//             }
//
//             fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error> where A: SeqAccess<'de> {
//                 let mut options = Options {
//                     expiry: None,
//                     existence: None,
//                     get: None,
//                 };
//
//                 while let Ok(Some(option)) = seq.next_element() {
//                     match option {
//                         OptionsEnum::Expiry(expiry) => {
//                             if options.expiry.is_some() {
//                                 return Err(Error::duplicate_field("Existence"));
//                             }
//                             options.expiry = Some(expiry)
//                         },
//                         OptionsEnum::Existence(existence) => {
//                             if options.existence.is_some() {
//                                 return Err(Error::duplicate_field("Existence"));
//                             }
//                             options.existence = Some(existence)
//                         },
//                         OptionsEnum::GET(get) => {
//                             if options.get.is_some() {
//                                 return Err(Error::duplicate_field("GET"));
//                             }
//                             options.get = Some(get)
//                         },
//                     }
//                 }
//
//                 Ok(options)
//             }
//         }
//
//         deserializer.deserialize_seq(OptionsVisitor)
//     }
// }