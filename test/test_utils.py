# -*- coding: utf-8 -*-
# Copyright (c) 2021-2025 Andrew Kane
# <see AUTHORS file>
#
# Copyright (C) 2021-2022 Huawei Technologies Co.,Ltd.
#
# This module is part of pgvector-python and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

import numpy as np
from struct import pack
from scipy.sparse import coo_array
import pytest

from opengauss_sqlalchemy.utils import Bit, Vector, SparseVector

class TestBit:
    def test_list(self):
        assert Bit([True, False, True]).to_list() == [True, False, True]

    def test_tuple(self):
        assert Bit((True, False, True)).to_list() == [True, False, True]

    def test_str(self):
        assert Bit('101').to_list() == [True, False, True]

    def test_ndarray_uint8(self):
        arr = np.array([254, 7, 0], dtype=np.uint8)
        assert Bit(arr).to_text() == '111111100000011100000000'

    def test_ndarray_uint16(self):
        arr = np.array([254, 7, 0], dtype=np.uint16)
        with pytest.raises(ValueError) as error:
            Bit(arr)
        assert str(error.value) == 'expected dtype to be bool or uint8'

    def test_ndarray_same_object(self):
        arr = np.array([True, False, True])
        assert Bit(arr).to_list() == [True, False, True]
        assert Bit(arr).to_numpy() is arr

    def test_ndim_two(self):
        with pytest.raises(ValueError) as error:
            Bit([[True, False], [True, False]])
        assert str(error.value) == 'expected ndim to be 1'

    def test_ndim_zero(self):
        with pytest.raises(ValueError) as error:
            Bit(True)
        assert str(error.value) == 'expected ndim to be 1'

    def test_repr(self):
        assert repr(Bit([True, False, True])) == 'Bit(101)'
        assert str(Bit([True, False, True])) == 'Bit(101)'

    def test_equality(self):
        assert Bit([True, False, True]) == Bit([True, False, True])
        assert Bit([True, False, True]) != Bit([True, False, False])

class TestSparseVector:
    def test_list(self):
        vec = SparseVector([1, 0, 2, 0, 3, 0])
        assert vec.to_list() == [1, 0, 2, 0, 3, 0]
        assert np.array_equal(vec.to_numpy(), [1, 0, 2, 0, 3, 0])
        assert vec.indices() == [0, 2, 4]

    def test_list_dimensions(self):
        with pytest.raises(ValueError) as error:
            SparseVector([1, 0, 2, 0, 3, 0], 6)
        assert str(error.value) == 'extra argument'

    def test_ndarray(self):
        vec = SparseVector(np.array([1, 0, 2, 0, 3, 0]))
        assert vec.to_list() == [1, 0, 2, 0, 3, 0]
        assert vec.indices() == [0, 2, 4]

    def test_dict(self):
        vec = SparseVector({2: 2, 4: 3, 0: 1, 3: 0}, 6)
        assert vec.to_list() == [1, 0, 2, 0, 3, 0]
        assert vec.indices() == [0, 2, 4]

    def test_dict_no_dimensions(self):
        with pytest.raises(ValueError) as error:
            SparseVector({0: 1, 2: 2, 4: 3})
        assert str(error.value) == 'missing dimensions'

    def test_coo_array(self):
        arr = coo_array(np.array([1, 0, 2, 0, 3, 0]))
        vec = SparseVector(arr)
        assert vec.to_list() == [1, 0, 2, 0, 3, 0]
        assert vec.indices() == [0, 2, 4]

    def test_coo_array_dimensions(self):
        with pytest.raises(ValueError) as error:
            SparseVector(coo_array(np.array([1, 0, 2, 0, 3, 0])), 6)
        assert str(error.value) == 'extra argument'

    def test_dok_array(self):
        arr = coo_array(np.array([1, 0, 2, 0, 3, 0])).todok()
        vec = SparseVector(arr)
        assert vec.to_list() == [1, 0, 2, 0, 3, 0]
        assert vec.indices() == [0, 2, 4]

    def test_repr(self):
        assert repr(SparseVector([1, 0, 2, 0, 3, 0])) == 'SparseVector({0: 1.0, 2: 2.0, 4: 3.0}, 6)'
        assert str(SparseVector([1, 0, 2, 0, 3, 0])) == 'SparseVector({0: 1.0, 2: 2.0, 4: 3.0}, 6)'

    def test_equality(self):
        assert SparseVector([1, 0, 2, 0, 3, 0]) == SparseVector([1, 0, 2, 0, 3, 0])
        assert SparseVector([1, 0, 2, 0, 3, 0]) != SparseVector([1, 0, 2, 0, 3, 1])
        assert SparseVector([1, 0, 2, 0, 3, 0]) == SparseVector({2: 2, 4: 3, 0: 1, 3: 0}, 6)
        assert SparseVector({}, 1) != SparseVector({}, 2)

    def test_dimensions(self):
        assert SparseVector([1, 0, 2, 0, 3, 0]).dimensions() == 6

    def test_indices(self):
        assert SparseVector([1, 0, 2, 0, 3, 0]).indices() == [0, 2, 4]

    def test_values(self):
        assert SparseVector([1, 0, 2, 0, 3, 0]).values() == [1, 2, 3]

    def test_to_coo(self):
        assert np.array_equal(SparseVector([1, 0, 2, 0, 3, 0]).to_coo().toarray(), [[1, 0, 2, 0, 3, 0]])

    def test_zero_vector_text(self):
        vec = SparseVector({}, 3)
        assert vec.to_list() == SparseVector.from_text(vec.to_text()).to_list()

    def test_from_text(self):
        vec = SparseVector.from_text('{1:1.5,3:2,5:3}/6')
        assert vec.dimensions() == 6
        assert vec.indices() == [0, 2, 4]
        assert vec.values() == [1.5, 2, 3]
        assert vec.to_list() == [1.5, 0, 2, 0, 3, 0]
        assert np.array_equal(vec.to_numpy(), [1.5, 0, 2, 0, 3, 0])

    def test_from_binary(self):
        data = pack('>iii3i3f', 6, 3, 0, 0, 2, 4, 1.5, 2, 3)
        vec = SparseVector.from_binary(data)
        assert vec.dimensions() == 6
        assert vec.indices() == [0, 2, 4]
        assert vec.values() == [1.5, 2, 3]
        assert vec.to_list() == [1.5, 0, 2, 0, 3, 0]
        assert np.array_equal(vec.to_numpy(), [1.5, 0, 2, 0, 3, 0])
        assert vec.to_binary() == data

class TestVector:
    def test_list(self):
        assert Vector([1, 2, 3]).to_list() == [1, 2, 3]

    def test_list_str(self):
        with pytest.raises(ValueError, match='could not convert string to float'):
            Vector([1, 'two', 3])

    def test_tuple(self):
        assert Vector((1, 2, 3)).to_list() == [1, 2, 3]

    def test_ndarray(self):
        arr = np.array([1, 2, 3])
        assert Vector(arr).to_list() == [1, 2, 3]
        assert Vector(arr).to_numpy() is not arr

    def test_ndarray_same_object(self):
        arr = np.array([1, 2, 3], dtype='>f4')
        assert Vector(arr).to_list() == [1, 2, 3]
        assert Vector(arr).to_numpy() is arr

    def test_ndim_two(self):
        with pytest.raises(ValueError) as error:
            Vector([[1, 2], [3, 4]])
        assert str(error.value) == 'expected ndim to be 1'

    def test_ndim_zero(self):
        with pytest.raises(ValueError) as error:
            Vector(1)
        assert str(error.value) == 'expected ndim to be 1'

    def test_repr(self):
        assert repr(Vector([1, 2, 3])) == 'Vector([1.0, 2.0, 3.0])'
        assert str(Vector([1, 2, 3])) == 'Vector([1.0, 2.0, 3.0])'

    def test_equality(self):
        assert Vector([1, 2, 3]) == Vector([1, 2, 3])
        assert Vector([1, 2, 3]) != Vector([1, 2, 4])

    def test_dimensions(self):
        assert Vector([1, 2, 3]).dimensions() == 3

    def test_from_text(self):
        vec = Vector.from_text('[1.5,2,3]')
        assert vec.to_list() == [1.5, 2, 3]
        assert np.array_equal(vec.to_numpy(), [1.5, 2, 3])

    def test_from_binary(self):
        data = pack('>HH3f', 3, 0, 1.5, 2, 3)
        vec = Vector.from_binary(data)
        assert vec.to_list() == [1.5, 2, 3]
        assert np.array_equal(vec.to_numpy(), [1.5, 2, 3])
        assert vec.to_binary() == data