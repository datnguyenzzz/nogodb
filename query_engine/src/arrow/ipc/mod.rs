pub mod schema;

use flatbuffers::{
    FlatBufferBuilder, InvalidFlatbuffer, Push, TableUnfinishedWIPOffset, UnionWIPOffset, VOffsetT,
    Verifiable, Verifier, WIPOffset,
};

use crate::arrow::DataType;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Precision(pub i16);
impl Precision {
    pub const HALF: Self = Self(0);
    pub const SINGLE: Self = Self(1);
    pub const DOUBLE: Self = Self(2);
}
impl Push for Precision {
    type Output = Precision;
    #[inline]
    unsafe fn push(&self, dst: &mut [u8], _written_len: usize) {
        unsafe {
            flatbuffers::emplace_scalar::<i16>(dst, self.0);
        }
    }
}

macro_rules! ipc_types {
    ($(
        $ty:ident {
            consts { $( $name:ident = $value:expr ),* $(,)? }
            functions { $($funcs:item)* }
        }
    ),*) => {
        $(
            struct $ty;
            impl $ty {
                $( pub const $name: VOffsetT = $value; )*
            }

            impl Verifiable for $ty {
                #[inline]
                fn run_verifier(v: &mut Verifier, pos: usize) -> Result<(), InvalidFlatbuffer> {
                    v.visit_table(pos)?.finish();
                    Ok(())
                }
            }

            paste::paste! {
                struct [<$ty Builder>]<'fbb: 'a, 'a> {
                    fbb: &'a mut FlatBufferBuilder<'fbb>,
                    start: WIPOffset<TableUnfinishedWIPOffset>
                }

                impl <'fbb: 'a, 'a> [<$ty Builder>]<'fbb, 'a>  {
                    #[inline]
                    pub fn new(fbb: &'a mut FlatBufferBuilder<'fbb>) -> Self {
                        let start = fbb.start_table();
                        Self {fbb, start}
                    }

                    #[inline]
                    pub fn finish(self) -> WIPOffset<$ty> {
                        let o = self.fbb.end_table(self.start);
                        WIPOffset::new(o.value())
                    }

                    $($funcs)*
                }
            }
        )*
    };
}

ipc_types! {
    Bool {consts {} functions {}},
    Int {
        consts {
            VT_BITWIDTH = 4,
            VT_IS_SIGNED = 6,
        }
        functions {
            #[inline]
            pub fn add_bit_width(&mut self, bit_width: i32) {
                self.fbb.push_slot::<i32>(Int::VT_BITWIDTH, bit_width, 0);
            }
            #[inline]
            pub fn add_is_signed(&mut self, is_signed: bool) {
                self.fbb.push_slot::<bool>(Int::VT_IS_SIGNED, is_signed, false);
            }
        }
    },
    FloatingPoint {
        consts {
            VT_PRECISION = 4,
        }
        functions {
            #[inline]
            pub fn add_precision(&mut self, precision: Precision) {
                self.fbb.push_slot::<Precision>(FloatingPoint::VT_PRECISION, precision, Precision::HALF);
            }
        }
    },
    Utf8 {consts{} functions {}}
    // Field_Type {
    //     VT_NAME = 4,
    //     VT_NULLABLE = 6,
    //     VT_TYPE = 10,
    // }
}

/// create IPC Field from arrow::Field
pub fn build_ipc_data_type(
    mut fbb: FlatBufferBuilder,
    data_type: &DataType,
) -> WIPOffset<UnionWIPOffset> {
    match data_type {
        DataType::Boolean => {
            let builder = BoolBuilder::new(&mut fbb);
            builder.finish().as_union_value()
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let mut builder = IntBuilder::new(&mut fbb);
            builder.add_is_signed(true);
            match data_type {
                DataType::Int8 => builder.add_bit_width(8),
                DataType::Int16 => builder.add_bit_width(16),
                DataType::Int32 => builder.add_bit_width(32),
                DataType::Int64 => builder.add_bit_width(64),
                _ => {}
            };
            builder.finish().as_union_value()
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let mut builder = FloatingPointBuilder::new(&mut fbb);
            match data_type {
                DataType::Float16 => builder.add_precision(Precision::HALF),
                DataType::Float32 => builder.add_precision(Precision::SINGLE),
                DataType::Float64 => builder.add_precision(Precision::DOUBLE),
                _ => {}
            }
            builder.finish().as_union_value()
        }
        DataType::Utf8 => {
            let builder = Utf8Builder::new(&mut fbb);
            builder.finish().as_union_value()
        }
    }
}
