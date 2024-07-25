use proc_macro::TokenStream;

use proc_macro2::TokenStream as TokenStr;
use quote::quote;
use syn::Ident;

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

    let variant_impls: Vec<TokenStr> = variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        variant_testing(name, variant_name)
    }).collect();

    let gen = quote! {

        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
            {

                // See https://stackoverflow.com/questions/75181286/how-to-implement-a-custom-deserializer-using-serde-that-allows-for-parsing-of-un/78793511#78793511
                struct NonSelfDescribingUntaggedEnumVisitor;

                impl<'de> serde::de::Visitor<'de> for NonSelfDescribingUntaggedEnumVisitor {
                    type Value = #name;

                    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str("one of the variants of the enum")
                    }

                    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> where E: serde::de::Error {
                        #(#variant_impls)*
                        Err(serde::de::Error::custom(format!("No fitting option found.")))
                    }

                }
                deserializer.deserialize_bytes(NonSelfDescribingUntaggedEnumVisitor)


            }
        }
    };
    gen.into()
}

// TODO: Expand to cases where variant fields != variant name
fn variant_testing(enum_name: &Ident, variant: &Ident) -> TokenStr {
    let gen = quote! {
            let variant: Result<#variant, crate::parser::ParseError> = crate::parser::from_slice(v);
            if let Ok(res) = variant {
            return Ok(#enum_name::#variant(res));
            }
    };

    gen.into()
}

//
// impl<'de> Deserialize<'de> for Options {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
//         // See https://stackoverflow.com/questions/75181286/how-to-implement-a-custom-deserializer-using-serde-that-allows-for-parsing-of-un/78793511#78793511
//         struct NonSelfDescribingUntaggedEnumVisitor;
//
//         impl<'de> Visitor<'de> for NonSelfDescribingUntaggedEnumVisitor {
//             type Value = Options;
//
//             fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
//                 formatter.write_str("One of the variants of the enum")
//             }
//
//             fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> where E: Error {
//                 let expiry: Result<Expiry, crate::parser::ParseError> = from_slice(v);
//                 if let Ok(res) = expiry {
//                     return Ok(Options::Expiry(res));
//                 }
//
//                 let existence: Result<Existence, crate::parser::ParseError> = from_slice(v);
//                 if let Ok(res) = existence {
//                     return Ok(Options::Existence(res));
//                 }
//
//                 let exp_err = expiry.unwrap_err();
//                 let exi_err = existence.unwrap_err();
//                 Err(serde::de::Error::custom(format!("No fitting option found. \nError for Expiry was: {}\nError for Existence was: {}", exp_err, exi_err)))
//             }
//         }
//
//         deserializer.deserialize_bytes(NonSelfDescribingUntaggedEnumVisitor)
//     }
// }