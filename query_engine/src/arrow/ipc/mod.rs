pub mod record_batch;
pub mod schema;

use std::sync::Arc;

use flatbuffers::{
    FlatBufferBuilder, Follow, ForwardsUOffset, InvalidFlatbuffer, Push, Table,
    TableUnfinishedWIPOffset, UnionWIPOffset, VOffsetT, Vector, Verifiable, Verifier, WIPOffset,
};

use crate::arrow::{DataType, Field, Fields};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct Precision(pub i16);
impl Precision {
    pub const HALF: Self = Self(0);
    pub const SINGLE: Self = Self(1);
    pub const DOUBLE: Self = Self(2);
}

impl<'a> Follow<'a> for Precision {
    type Inner = Self;
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        let b = unsafe { flatbuffers::read_scalar_at::<i16>(buf, loc) };
        Self(b)
    }
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

macro_rules! define_ipc_types {
    ($(
        $ty:ident {
            consts { $( $name:ident = $value:expr ),* $(,)? }
            functions { $($funcs:item)* }
            builder_functions { $($b_funcs:item)* }
        }
    ),*) => {
        pub struct FbType;

        #[repr(i8)]
        enum AggIndex {
            $($ty),*
        }

        #[allow(non_upper_case_globals)]
        impl FbType {
            $( pub const $ty: i8 = AggIndex::$ty as i8; )*
        }

        $(
            pub struct $ty<'a> {
                _tab: Table<'a>
            }

            impl <'a> $ty <'a> {
                $( pub const $name: VOffsetT = $value; )*

                #[inline]
                pub fn from_table(table: Table<'a>) -> Self {
                    Self {_tab: table}
                }

                $($funcs)*
            }

            impl Verifiable for $ty<'_> {
                #[inline]
                fn run_verifier(v: &mut Verifier, pos: usize) -> Result<(), InvalidFlatbuffer> {
                    v.visit_table(pos)?.finish();
                    Ok(())
                }
            }

            impl <'a> Follow <'a> for $ty<'a> {
                type Inner = $ty<'a>;
                #[inline]
                unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
                    Self {
                        _tab: unsafe { Table::new(buf, loc) },
                    }
                }
            }

            paste::paste! {
                pub struct [<$ty Builder>]<'fbb: 'a, 'a> {
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
                    pub fn finish(self) -> WIPOffset<$ty<'fbb>> {
                        let o = self.fbb.end_table(self.start);
                        WIPOffset::new(o.value())
                    }

                    $($b_funcs)*
                }
            }
        )*
    };
}

define_ipc_types! {
    Unknown {consts {} functions{} builder_functions {}},
    Bool {consts {} functions{} builder_functions {}},
    Int {
        consts {
            VT_BITWIDTH = 4,
            VT_IS_SIGNED = 6,
        }
        functions{
            #[inline]
            pub fn bit_width(&self) -> Option<i32> {
                // Safety:
                // Created from valid Table for this object
                // which contains a valid value in this slot
                unsafe {
                    self._tab.get::<i32>(Int::VT_BITWIDTH, Some(0))
                }
            }
            #[inline]
            pub fn is_signed(&self) -> Option<bool> {
                // Safety:
                // Created from valid Table for this object
                // which contains a valid value in this slot
                unsafe {
                    self._tab.get::<bool>(Int::VT_IS_SIGNED, Some(false))
                }
            }
        }
        builder_functions {
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
        functions{
            #[inline]
            pub fn precision(&self) -> Option<Precision> {
                // Safety:
                // Created from valid Table for this object
                // which contains a valid value in this slot
                unsafe {
                    self._tab.get::<Precision>(FloatingPoint::VT_PRECISION, None)
                }
            }
        }
        builder_functions {
            #[inline]
            pub fn add_precision(&mut self, precision: Precision) {
                self.fbb.push_slot::<Precision>(FloatingPoint::VT_PRECISION, precision, Precision::HALF);
            }
        }
    },
    Utf8 {consts{} functions{} builder_functions {}},
    FbField {
        consts {
            VT_NAME = 4,
            VT_NULLABLE = 6,
            VT_TYPE_TYPE = 8,
            VT_TYPE = 10,
        }
        functions{
            #[inline]
            pub fn name(&self) -> Option<&'a str> {
                // Safety:
                // Created from valid Table for this object
                // which contains a valid value in this slot
                unsafe {
                    self._tab.get::<ForwardsUOffset<&'a str>>(FbField::VT_NAME, None)
                }
            }
            #[inline]
            pub fn nullable(&self) -> Option<bool> {
                // Safety:
                // Created from valid Table for this object
                // which contains a valid value in this slot
                unsafe {
                    self._tab.get::<bool>(FbField::VT_NULLABLE, Some(false))
                }
            }
            #[inline]
            pub fn type_type(&self) -> Option<i8> {
                // Safety:
                // Created from valid Table for this object
                // which contains a valid value in this slot
                unsafe {
                    self._tab.get::<i8>(FbField::VT_TYPE_TYPE, Some(FbType::Unknown))
                }
            }
            #[inline]
            pub fn type_val(&self) -> Option<Table<'a>> {
                // Safety:
                // Created from valid Table for this object
                // which contains a valid value in this slot
                unsafe {
                    self._tab.get::<ForwardsUOffset<Table<'a>>>(FbField::VT_TYPE, None)
                }
            }
            #[inline]
            pub fn as_int(&self) -> Option<Int<'a>> {
                if self.type_type() == Some(FbType::Int) {
                    self.type_val().map(|tab| Int::from_table(tab))
                } else {
                    None
                }
            }
            #[inline]
            pub fn as_floating_point(&self) -> Option<FloatingPoint<'a>> {
                if self.type_type() == Some(FbType::FloatingPoint) {
                    self.type_val().map(|tab| FloatingPoint::from_table(tab))
                } else {
                    None
                }
            }
        }
        builder_functions{
            #[inline]
            pub fn push_name(&mut self, name: WIPOffset<&'a str>) {
                self.fbb.push_slot_always::<WIPOffset<_>>(FbField::VT_NAME, name);
            }
            #[inline]
            pub fn push_nullable(&mut self, nullable: bool) {
                self.fbb.push_slot::<bool>(FbField::VT_NULLABLE, nullable, false);
            }
            #[inline]
            pub fn push_type_type(&mut self, ty: i8) {
                self.fbb.push_slot::<i8>(FbField::VT_TYPE_TYPE, ty, FbType::Unknown);
            }
            #[inline]
            pub fn push_type(&mut self, ty: WIPOffset<UnionWIPOffset>) {
                self.fbb.push_slot_always::<WIPOffset<_>>(FbField::VT_TYPE, ty);
            }
        }
    },
    FbSchema {
        consts {
            VT_FIELDS = 4,
        }
        functions{
            #[inline]
            pub fn fields(&self) -> Option<Vector<'a, ForwardsUOffset<FbField<'a>>>> {
                // Safety:
                // Created from valid Table for this object
                // which contains a valid value in this slot
                unsafe {
                    self._tab.get::<ForwardsUOffset<Vector<'a, ForwardsUOffset<FbField>>>>(FbSchema::VT_FIELDS, None)
                }
            }
        }
        builder_functions {
            #[inline]
            pub fn push_fields(&mut self, fields: WIPOffset<Vector<'fbb, ForwardsUOffset<FbField>>>) {
                self.fbb.push_slot_always::<WIPOffset<_>>(FbSchema::VT_FIELDS, fields);
            }
        }
    },
    FbMetaNode {
        consts {
            VT_LENGTH = 4,
            VT_NULL_COUNT = 6,
            VT_SEGMENT_OFFSET = 8,
            VT_SEGMENT_LENGTH = 10,
        }
        functions {
            #[inline]
            pub fn length(&self) -> i64 {
                unsafe { self._tab.get::<i64>(FbMetaNode::VT_LENGTH, Some(0)).unwrap() }
            }

            #[inline]
            pub fn null_count(&self) -> i64 {
                unsafe { self._tab.get::<i64>(FbMetaNode::VT_NULL_COUNT, Some(0)).unwrap() }
            }
        }
        builder_functions {
            #[inline]
            pub fn push_length(&mut self, len: i64) {
                self.fbb.push_slot::<i64>(FbMetaNode::VT_LENGTH, len, 0);
            }
            #[inline]
            pub fn push_null_count(&mut self, count: i64) {
                self.fbb.push_slot::<i64>(FbMetaNode::VT_NULL_COUNT, count, 0);
            }
        }
    },
    FbMetaSegment {
        consts {
            VT_OFFSET = 4,
            VT_LENGTH = 6,
        }
        functions {
            #[inline]
            pub fn offset(&self) -> i64 {
                unsafe { self._tab.get::<i64>(FbMetaSegment::VT_OFFSET, Some(0)).unwrap() }
            }

            #[inline]
            pub fn length(&self) -> i64 {
                unsafe { self._tab.get::<i64>(FbMetaSegment::VT_LENGTH, Some(0)).unwrap() }
            }
        }
        builder_functions {
            #[inline]
            pub fn push_offset(&mut self, off: i64) {
                self.fbb.push_slot::<i64>(FbMetaSegment::VT_OFFSET, off, 0);
            }
            #[inline]
            pub fn push_length(&mut self, len: i64) {
                self.fbb.push_slot::<i64>(FbMetaSegment::VT_LENGTH, len, 0);
            }
        }
    },
    FbRecordBatch {
        consts {
            VT_LENGTH = 4,
            VT_NODES = 6,
            VT_SEGMENTS = 8,
            VT_COMPRESSION = 10,
        }
        functions {
            #[inline]
            pub fn length(&self) -> i32 {
                unsafe { self._tab.get::<i32>(FbRecordBatch::VT_LENGTH, Some(0)).unwrap() }
            }

            #[inline]
            pub fn nodes(&self) -> Option<Vector<'a, ForwardsUOffset<FbMetaNode<'a>>>> {
                unsafe {
                    self._tab.get::<ForwardsUOffset<Vector<'a, ForwardsUOffset<FbMetaNode>>>>(FbRecordBatch::VT_NODES, None)
                }
            }

            #[inline]
            pub fn segments(&self) -> Option<Vector<'a, ForwardsUOffset<FbMetaSegment<'a>>>> {
                unsafe {
                    self._tab.get::<ForwardsUOffset<Vector<'a, ForwardsUOffset<FbMetaSegment>>>>(FbRecordBatch::VT_SEGMENTS, None)
                }
            }

            #[inline]
            pub fn compression(&self) -> i32 {
                unsafe {
                    self._tab.get::<i32>(FbRecordBatch::VT_COMPRESSION, Some(0)).unwrap()
                }
            }
        }
        builder_functions {
            #[inline]
            pub fn push_length(&mut self, len: i32) {
                self.fbb.push_slot::<i32>(FbRecordBatch::VT_LENGTH, len, 0);
            }
            #[inline]
            pub fn push_nodes(&mut self, nodes: WIPOffset<Vector<'fbb, ForwardsUOffset<FbMetaNode>>>) {
                self.fbb.push_slot_always::<WIPOffset<_>>(FbRecordBatch::VT_NODES, nodes);
            }
            #[inline]
            pub fn push_segments(&mut self, segments: WIPOffset<Vector<'fbb, ForwardsUOffset<FbMetaSegment>>>) {
                self.fbb.push_slot_always::<WIPOffset<_>>(FbRecordBatch::VT_SEGMENTS, segments);
            }
            #[inline]
            pub fn push_compression(&mut self, compression: i32) {
                self.fbb.push_slot::<i32>(FbRecordBatch::VT_COMPRESSION, compression, 0 /*uncompressed*/);
            }
        }
    },
    FbMessage {
        consts {
            VT_VERSION = 4,
            VT_HEADER_TYPE = 6,
            VT_HEADER = 8,
            VT_BODY_LENGTH = 10,
        }

        functions {
            #[inline]
            pub fn version(&self) -> i16 {
                unsafe { self._tab.get::<i16>(FbMessage::VT_VERSION, Some(0)).unwrap() }
            }
            #[inline]
            pub fn header_type(&self) -> u8 {
                // 1 = Schema, 2 = RecordBatch
                unsafe { self._tab.get::<u8>(FbMessage::VT_HEADER_TYPE, Some(0)).unwrap() }
            }
            #[inline]
            pub fn header(&self) -> Option<Table<'a>> {
                unsafe { self._tab.get::<ForwardsUOffset<Table<'a>>>(FbMessage::VT_HEADER, None) }
            }
            #[inline]
            pub fn header_as_fb_schema(&self) -> Option<FbSchema<'a>> {
                if self.header_type() == 1 {
                    self.header().map(|t| FbSchema::from_table(t))
                } else {
                    None
                }
            }
            #[inline]
            pub fn header_as_fb_record_batch(&self) -> Option<FbRecordBatch<'a>> {
                if self.header_type() == 2 {
                    self.header().map(|t| FbRecordBatch::from_table(t))
                } else {
                    None
                }
            }
            #[inline]
            pub fn body_length(&self) -> i64 {
                unsafe { self._tab.get::<i64>(FbMessage::VT_BODY_LENGTH, Some(0)).unwrap() }
            }
        }
        builder_functions {
            #[inline]
            pub fn push_version(&mut self, version: i16) {
                self.fbb.push_slot::<i16>(FbMessage::VT_VERSION, version, 0);
            }
            #[inline]
            pub fn push_header_type(&mut self, ty: u8) {
                self.fbb.push_slot::<u8>(FbMessage::VT_HEADER_TYPE, ty, 0);
            }
            #[inline]
            pub fn push_header(&mut self, header: WIPOffset<UnionWIPOffset>) {
                self.fbb.push_slot_always::<WIPOffset<_>>(FbMessage::VT_HEADER, header);
            }
            #[inline]
            pub fn push_body_length(&mut self, len: i64) {
                self.fbb.push_slot::<i64>(FbMessage::VT_BODY_LENGTH, len, 0);
            }
        }
    }
}

/// create IPC Field from arrow::Field
pub fn build_ipc_data_type(
    fbb: &mut FlatBufferBuilder,
    data_type: &DataType,
) -> (i8, WIPOffset<UnionWIPOffset>) {
    match data_type {
        DataType::Boolean => {
            let builder = BoolBuilder::new(fbb);
            (FbType::Bool, builder.finish().as_union_value())
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let mut builder = IntBuilder::new(fbb);
            builder.add_is_signed(true);
            match data_type {
                DataType::Int8 => builder.add_bit_width(8),
                DataType::Int16 => builder.add_bit_width(16),
                DataType::Int32 => builder.add_bit_width(32),
                DataType::Int64 => builder.add_bit_width(64),
                _ => {}
            };
            (FbType::Int, builder.finish().as_union_value())
        }
        DataType::Float32 | DataType::Float64 => {
            let mut builder = FloatingPointBuilder::new(fbb);
            match data_type {
                DataType::Float32 => builder.add_precision(Precision::SINGLE),
                DataType::Float64 => builder.add_precision(Precision::DOUBLE),
                _ => {}
            }
            (FbType::FloatingPoint, builder.finish().as_union_value())
        }
        DataType::Utf8 => {
            let builder = Utf8Builder::new(fbb);
            (FbType::Utf8, builder.finish().as_union_value())
        }
    }
}

impl<'a> FromIterator<FbField<'a>> for Fields {
    fn from_iter<T: IntoIterator<Item = FbField<'a>>>(iter: T) -> Self {
        let fields: Vec<_> = iter
            .into_iter()
            .map(|fb| Arc::new(Field::from(fb)))
            .collect();

        Self(Arc::from(fields))
    }
}

impl From<Vec<FbField<'_>>> for Fields {
    fn from(fb_fields: Vec<FbField>) -> Self {
        let fields: Vec<_> = fb_fields
            .into_iter()
            .map(|fb| Arc::new(Field::from(fb)))
            .collect();

        Self(Arc::from(fields))
    }
}

/// Convert IPC Field to arrow:Field
impl<'a> From<FbField<'a>> for Field {
    fn from(fb_field: FbField<'a>) -> Self {
        let data_type = match fb_field.type_type().unwrap_or_default() {
            FbType::Bool => DataType::Boolean,
            FbType::Int => {
                let int = fb_field.as_int().unwrap();
                if int.is_signed() == Some(false) {
                    panic!("Unsigned int is not supported yet")
                }
                match int.bit_width().unwrap_or_default() {
                    8 => DataType::Int8,
                    16 => DataType::Int16,
                    32 => DataType::Int32,
                    64 => DataType::Int64,
                    e => panic!("corrupted bit width: {} when casting Int field", e),
                }
            }
            FbType::FloatingPoint => {
                let float = fb_field.as_floating_point().unwrap();
                match float.precision() {
                    None => panic!("precision is missing from the floating point field"),
                    Some(p) => match p {
                        Precision::SINGLE => DataType::Float32,
                        Precision::DOUBLE => DataType::Float64,
                        e => panic!("precision {e:?} is unexpected"),
                    },
                }
            }
            FbType::Utf8 => DataType::Utf8,
            _ => panic!("unknown data_type when casting FbField -> Field"),
        };

        Self {
            name: fb_field.name().unwrap_or_default().to_string(),
            data_type,
            nullable: fb_field.nullable().unwrap_or_default(),
        }
    }
}
