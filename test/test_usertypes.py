from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.sql import select
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from opengauss_sqlalchemy import dc_psycopg2, psycopg2
from opengauss_sqlalchemy.utils import Vector, Bit, SparseVector
from opengauss_sqlalchemy.usertype import BIT, SPARSEVEC, VECTOR

m = MetaData()
tbl = Table(
    "test",
    m,
    Column("id", Integer),
    Column("bit_embedding", BIT(3)),
    Column("sparsevec_embedding", SPARSEVEC(3)),
    Column("vector_embedding", VECTOR(3))
)

class TestBit(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = psycopg2.dialect()

    def test_bit_get_col_spec(self):
        bit = BIT()
        assert bit.get_col_spec() == 'BIT'
        bit_with_length = BIT(5)
        assert bit_with_length.get_col_spec() == 'BIT(5)'
    
    def test_bit_distance(self):
        hamming_stmt = select(tbl.c.id).order_by(tbl.c.bit_embedding.hamming_distance('110'))
        self.assert_compile(
            hamming_stmt,
            "SELECT test.id FROM test ORDER BY test.bit_embedding <~> %(bit_embedding_1)s",
            checkparams = {"bit_embedding_1" : '110'}
        )

        jaccard_stmt = select(tbl.c.id).order_by(tbl.c.bit_embedding.jaccard_distance('110'))
        self.assert_compile(
            jaccard_stmt,
            "SELECT test.id FROM test ORDER BY test.bit_embedding <%%> %(bit_embedding_1)s",
            checkparams = {"bit_embedding_1" : '110'}
        )

class TestSparseVec(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = psycopg2.dialect()

    def test_sparsevec_get_col_spec(self):
        sparsevec = SPARSEVEC()
        assert sparsevec.get_col_spec() == 'SPARSEVEC'
        sparsevec_with_dim = SPARSEVEC(5)
        assert sparsevec_with_dim.get_col_spec() == 'SPARSEVEC(5)'

    def test_sparsevec_distance(self):
        l2_stmt = select(tbl.c.id).order_by(tbl.c.sparsevec_embedding.l2_distance(SparseVector([1, 2, 3])))
        self.assert_compile(
            l2_stmt,
            "SELECT test.id FROM test ORDER BY test.sparsevec_embedding <-> %(sparsevec_embedding_1)s",
            checkparams = {"sparsevec_embedding_1" : SparseVector([1, 2, 3])}
        )

        max_inner_product_stmt = select(tbl.c.id).order_by(tbl.c.sparsevec_embedding.max_inner_product(SparseVector([1, 2, 3])))
        self.assert_compile(
            max_inner_product_stmt,
            "SELECT test.id FROM test ORDER BY test.sparsevec_embedding <#> %(sparsevec_embedding_1)s",
            checkparams = {"sparsevec_embedding_1" : SparseVector([1, 2, 3])}
        )

        cosine_stmt = select(tbl.c.id).order_by(tbl.c.sparsevec_embedding.cosine_distance(SparseVector([1, 2, 3])))
        self.assert_compile(
            cosine_stmt,
            "SELECT test.id FROM test ORDER BY test.sparsevec_embedding <=> %(sparsevec_embedding_1)s",
            checkparams = {"sparsevec_embedding_1" : SparseVector([1, 2, 3])}
        )

        l1_stmt = select(tbl.c.id).order_by(tbl.c.sparsevec_embedding.l1_distance(SparseVector([1, 2, 3])))
        self.assert_compile(
            l1_stmt,
            "SELECT test.id FROM test ORDER BY test.sparsevec_embedding <+> %(sparsevec_embedding_1)s",
            checkparams = {"sparsevec_embedding_1" : SparseVector([1, 2, 3])}
        )

    def test_sparsevec_literal_binds(self):
        sql = select(tbl.c.id).order_by(tbl.c.sparsevec_embedding.l2_distance(SparseVector([1, 2, 3])))\
            .compile(compile_kwargs = {'literal_binds' : True})
        assert "embedding <-> '{1:1.0,2:2.0,3:3.0}/3'" in str(sql)


class TestVector(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = psycopg2.dialect()

    def test_vector_get_col_spec(self):
        vec = VECTOR()
        assert vec.get_col_spec() == 'VECTOR'
        vec_with_dim = VECTOR(5)
        assert vec_with_dim.get_col_spec() == 'VECTOR(5)'

    def test_vector_distance(self):
        l2_stmt = select(tbl.c.id).order_by(tbl.c.vector_embedding.l2_distance([1,2,3]))
        self.assert_compile(
            l2_stmt,
            "SELECT test.id FROM test ORDER BY test.vector_embedding <-> %(vector_embedding_1)s",
            checkparams = {"vector_embedding_1" : [1,2,3]}
        )

        max_inner_product_stmt = select(tbl.c.id).order_by(tbl.c.vector_embedding.max_inner_product([1,2,3]))
        self.assert_compile(
            max_inner_product_stmt,
            "SELECT test.id FROM test ORDER BY test.vector_embedding <#> %(vector_embedding_1)s",
            checkparams = {"vector_embedding_1" : [1,2,3]}
        )

        cosine_stmt = select(tbl.c.id).order_by(tbl.c.vector_embedding.cosine_distance([1,2,3]))
        self.assert_compile(
            cosine_stmt,
            "SELECT test.id FROM test ORDER BY test.vector_embedding <=> %(vector_embedding_1)s",
            checkparams = {"vector_embedding_1" : [1,2,3]}
        )

        l1_stmt = select(tbl.c.id).order_by(tbl.c.vector_embedding.l1_distance([1,2,3]))
        self.assert_compile(
            l1_stmt,
            "SELECT test.id FROM test ORDER BY test.vector_embedding <+> %(vector_embedding_1)s",
            checkparams = {"vector_embedding_1" : [1,2,3]}
        )

    def test_vector_literal_binds(self):
        sql = select(tbl.c.id).order_by(tbl.c.vector_embedding.l2_distance([1, 2, 3]))\
            .compile(compile_kwargs = {'literal_binds' : True})
        assert "embedding <-> '[1.0,2.0,3.0]'" in str(sql)