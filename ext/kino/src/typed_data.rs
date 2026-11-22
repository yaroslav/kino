//! Kino's native types as Ruby sees them: wrapped as `#[magnus::wrap]` wraps, with their frees
//! declared thread-safe where the Ruby built against has the flag (build.rs). A Ruby with
//! per-Ractor GC then frees them in their Ractor's own sweep, not in a process-wide batch under a
//! VM barrier.
//!
//! The flag is a promise each type listed here keeps (doc/architecture.md). Its drop, and the
//! drop of all it owns or reaches, may run on any thread, a Ruby thread or not, beside other frees
//! and running Ractors: no Ruby API and no read of a Ruby object, only Send and Sync state, no
//! lock that anything holds across a Ruby call or a GVL release, no panic, and nothing that
//! blocks or waits (no join, no blocking send, no runtime dropped).

use magnus::typed_data::DataTypeBuilder;
use magnus::value::{Lazy, ReprValue};
use magnus::{Class, DataType, RClass, Ruby, TypedData};
use rb_sys::{rb_data_type_t, VALUE};

use crate::inline::ConnHandle;
use crate::pin::PinKeeper;
use crate::queue::Worker;
use crate::request::Request;

/// RUBY_TYPED_THREAD_SAFE_FREE, or no bit where Ruby has no such flag.
#[cfg(kino_thread_safe_free)]
const THREAD_SAFE_FREE: VALUE =
    rb_sys::rbimpl_typeddata_flags::RUBY_TYPED_THREAD_SAFE_FREE as VALUE;
#[cfg(not(kino_thread_safe_free))]
const THREAD_SAFE_FREE: VALUE = 0;

/// `built` with THREAD_SAFE_FREE, which magnus's builder cannot set; all else as it was built.
const fn thread_safe_free(built: DataType) -> DataType {
    // SAFETY: DataType is #[repr(transparent)] over rb_data_type_t, so it is the same value
    // either way. A magnus whose DataType changes size fails to build here.
    unsafe {
        let mut raw = std::mem::transmute::<DataType, rb_data_type_t>(built);
        raw.flags |= THREAD_SAFE_FREE;
        std::mem::transmute::<rb_data_type_t, DataType>(raw)
    }
}

/// `magnus::TypedData` for each `$ty` as `#[magnus::wrap(class = $class, $flag, ...)]` derives
/// it, but for the DataType, which goes through thread_safe_free.
macro_rules! native_types {
    ($($ty:ty: $class:literal $(, $flag:ident)*;)*) => {
        $(
            // SAFETY: magnus's own derive for these attributes, plus a flag whose promise the
            // type's drop keeps (module docs).
            unsafe impl TypedData for $ty {
                fn class(ruby: &Ruby) -> RClass {
                    static CLASS: Lazy<RClass> = Lazy::new(|ruby| {
                        let name = $class.to_str().expect("an ASCII class name");
                        let class: RClass =
                            ruby.class_object().funcall("const_get", (name,)).unwrap();
                        class.undef_default_alloc_func();
                        class
                    });
                    ruby.get_inner(&CLASS)
                }

                fn data_type() -> &'static DataType {
                    static DATA_TYPE: DataType = thread_safe_free(
                        DataTypeBuilder::<$ty>::new($class)$(.$flag())*.build(),
                    );
                    &DATA_TYPE
                }
            }
        )*

        /// Resolves every class now, on the main Ractor: magnus resolves one lazily at its first
        /// wrap, and two worker Ractors must never race that.
        pub fn resolve_classes(ruby: &Ruby) {
            $(let _ = <$ty>::class(ruby);)*
        }
    };
}

// Why each drop keeps the promise is in its type's docs.
native_types! {
    Request: c"Kino::Native::Request", free_immediately;
    ConnHandle: c"Kino::Native::Conn", free_immediately;
    Worker: c"Kino::Native::Worker", free_immediately;
    PinKeeper: c"Kino::Native::PinKeeper", mark, free_immediately;
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use magnus::DataTypeFunctions;

    use super::*;

    /// What Ruby reads of `data_type`.
    fn raw(data_type: &DataType) -> rb_data_type_t {
        // SAFETY: as thread_safe_free, here a bitwise copy of pointers and integers.
        unsafe { std::mem::transmute_copy(data_type) }
    }

    /// `T` is `class` as magnus builds it freed immediately with `attrs`, plus the flag.
    fn wrapped<T: TypedData + DataTypeFunctions>(
        class: &'static CStr,
        attrs: fn(DataTypeBuilder<T>) -> DataTypeBuilder<T>,
    ) {
        let ours = raw(T::data_type());
        let magnus = raw(&attrs(DataTypeBuilder::new(class))
            .free_immediately()
            .build());
        // SAFETY: native_types! names a type with a C string literal.
        let name = unsafe { CStr::from_ptr(ours.wrap_struct_name) };
        assert_eq!(name, class);
        assert_eq!(
            (ours.function.dmark.is_some(), ours.function.dfree.is_some()),
            (magnus.function.dmark.is_some(), true),
            "{class:?}: whether it has a mark and a free function"
        );
        assert_eq!(
            ours.flags,
            magnus.flags | THREAD_SAFE_FREE,
            "{class:?}: flags other than magnus's, plus thread-safe free where Ruby has it \
             (cfg kino_thread_safe_free: {})",
            cfg!(kino_thread_safe_free)
        );
    }

    #[test]
    fn native_types_are_wrapped_as_magnus_wraps_them_plus_a_thread_safe_free_where_ruby_has_it() {
        wrapped::<Request>(c"Kino::Native::Request", |plain| plain);
        wrapped::<ConnHandle>(c"Kino::Native::Conn", |plain| plain);
        wrapped::<Worker>(c"Kino::Native::Worker", |plain| plain);
        wrapped::<PinKeeper>(c"Kino::Native::PinKeeper", DataTypeBuilder::mark);
    }
}
