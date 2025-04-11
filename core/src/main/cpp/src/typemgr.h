/*
 * SPDX-License-Identifier: SSPL-1.0
 *
 * The Lucenia project is source-available software: you can
 * redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 *
 * As per the terms of the Server Side Public License, if you
 * make the functionality of this program or a modified version
 * available over a network, you must make the source code
 * available for download.
 *
 * The full text of the Server Side Public License, version 1,
 * can be found at:
 * https://www.mongodb.com/licensing/server-side-public-license
 */

#pragma once

#include "pch.h"

namespace xtree {

    class InvalidXtType {};

    /*template< typename T >
    struct xtTypeTraitBase {
        static const int size(T v)=0;
    };*/

    template< typename T_ >
    struct xtTypeTraits /*: xtTypeTraitBase<T_>*/;

    template<>
    struct xtTypeTraits<string> {
        static long unsigned int length(string value) { return value.length(); }
        static void setValue(string rhs, string &lhs) { lhs = rhs;  }
        /*static int compare(string v1, string v2) {

        }*/
    };

    template<>
    struct xtTypeTraits<int> {
        static int length(int) { return -1; }
        static void setValue(int rhs, string &lhs) {
            ostringstream strs;
            strs << rhs;
            string str = strs.str();
        }
    };

    template<>
    struct xtTypeTraits<long> {
        static int length(long) { return -1; }
        static void setValue(long rhs, string &lhs) {
            ostringstream strs;
            strs << rhs;
            string str = strs.str();
        }
    };

    template<>
    struct xtTypeTraits<float> {
        static int length(float) { return -1; }
        static void setValue(float rhs, string &lhs) {
            ostringstream strs;
            strs << rhs;
            string str = strs.str();
        }
    };

    template<>
    struct xtTypeTraits<double> {
        static int length(double) { return -1; }
        static void setValue(double rhs, string &lhs) {
            ostringstream strs;
            strs << rhs;
            string str = strs.str();
        }
    };

    class xtType {
    private:
        /** retrieve the next magic number automagically */
        static int next_magic_number() {
            static int magic(0);
            return magic++;
        }

        template< typename T_ >
        static int magic_number_for() {
            static int result(next_magic_number());
            return result;
        }

        template< typename T_ >
        static int sizeof_for() {
            static int result(sizeof(T_));
            return result;
        }

        /**
         * Base structure for the xtType Runtime Type Checking Utility
         */
        struct xtTypeValueBase {
            int magic_number;
            int size;
            int length;

            xtTypeValueBase( const int m, const int s, const int sf ) : magic_number(m), size(s), length(sf) {}
            virtual ~xtTypeValueBase() {}
        };

        template< typename T_ >
        struct xtTypeValue : xtTypeValueBase {
            T_ value;
            string val;

            xtTypeValue( const T_ &v ) : xtTypeValueBase(magic_number_for<T_>(), sizeof_for<T_>(), xtTypeTraits<T_>::length(v)), value(v) {
                xtTypeTraits<T_>::setValue(v, val);
            }
        };

        std::shared_ptr<xtTypeValueBase> _value;

    public:


        template< typename T_ >
        xtType( const T_ &t ) : _value(new xtTypeValue<T_>(t)) {}

        template< typename T_ >
        const T_& as() const {
            if( magic_number_for<T_>() != _value->magic_number )
                throw InvalidXtType();
            return std::static_pointer_cast< const xtTypeValue<T_> >(_value)->value;
        }

        const int type() {
            cout << "sizeof: " << _value->size << " length: " << _value->length << " ";
            return _value->magic_number;
        }

        /*template< typename T_ >
        const T_& value() const {
            return xtTypeTraits<T_>::getValue(_);
        }*/

    }; // class xtType
} // namespace xtree
