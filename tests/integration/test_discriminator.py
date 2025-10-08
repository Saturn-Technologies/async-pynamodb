import uuid

import pytest

from pynamodb.attributes import DiscriminatorRangeKeyAttribute, UnicodeAttribute, DiscriminatorAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex
from pynamodb.models import Model


class TestDiscriminator:
    class Index(GlobalSecondaryIndex):
        class Meta:
            index_name = 'index'
            projection = AllProjection()

        id = UnicodeAttribute(hash_key=True)
        cls = DiscriminatorRangeKeyAttribute()

    class ParentModel(Model):
        class Meta:
            table_name = 'secondary_discriminator_range_key_test'

        id = UnicodeAttribute(hash_key=True)
        other_id = UnicodeAttribute(range_key=True)
        cls = DiscriminatorAttribute()

    class ChildModelA(ParentModel, discriminator='child_a'):
        pass

    class ChildModelB(ParentModel, discriminator='child_b'):
        pass

    @pytest.fixture(scope='module', autouse=True)
    def create_tables(self, ddb_url):
        TestDiscriminator.ParentModel.Meta.host = ddb_url
        TestDiscriminator.ParentModel.create_table(
            read_capacity_units=10,
            write_capacity_units=10,
            wait=True
        )
        yield

        if TestDiscriminator.ParentModel.exists():
            TestDiscriminator.ParentModel.delete_table()

    @pytest.mark.ddblocal
    def test_query_with_parent(self):
        id_ = str(uuid.uuid4())
        child_a = TestDiscriminator.ChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_a.save()
        child_b = TestDiscriminator.ChildModelB(id=id_, other_id=str(uuid.uuid4()))
        child_b.save()

        all_children = list(TestDiscriminator.ParentModel.query(hash_key=id_))
        assert len(all_children) == 2
        assert any(isinstance(child, TestDiscriminator.ChildModelA) for child in all_children)
        assert any(isinstance(child, TestDiscriminator.ChildModelB) for child in all_children)

    @pytest.mark.ddblocal
    def test_query(self):
        id_ = str(uuid.uuid4())
        child_a = TestDiscriminator.ChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_a.save()
        child_b = TestDiscriminator.ChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_b.save()

        children_a = list(TestDiscriminator.ChildModelA.query(
            hash_key=id_, range_key_condition=TestDiscriminator.ChildModelA.other_id == child_a.other_id
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, TestDiscriminator.ChildModelA) for child in children_a)

    @pytest.mark.ddblocal
    def test_query_implicit_filter(self):
        id_ = str(uuid.uuid4())
        child_a = TestDiscriminator.ChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_a.save()
        child_b = TestDiscriminator.ChildModelB(id=id_, other_id=str(uuid.uuid4()))
        child_b.save()

        # ChildModelB should be implcitly filtered out.
        children_a = list(TestDiscriminator.ChildModelA.query(
            hash_key=id_
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, TestDiscriminator.ChildModelA) for child in children_a)

        # ChildModelA should be implcitly filtered out.
        children_b = list(TestDiscriminator.ChildModelB.query(
            hash_key=id_, range_key_condition=TestDiscriminator.ParentModel.other_id == child_a.other_id
        ))
        assert len(children_b) == 0


class TestPrimaryDiscriminatorRangeKey:
    class ParentModel(Model):
        class Meta:
            table_name = 'primary_discriminator_range_key_test'

        id = UnicodeAttribute(hash_key=True)
        cls = DiscriminatorRangeKeyAttribute()

    class ChildModelA(ParentModel, discriminator='child_a'):
        pass

    class ChildModelB(ParentModel, discriminator='child_b'):
        pass

    @pytest.fixture(scope='module', autouse=True)
    def create_tables(self, ddb_url):
        TestPrimaryDiscriminatorRangeKey.ParentModel.Meta.host = ddb_url
        TestPrimaryDiscriminatorRangeKey.ParentModel.create_table(
            read_capacity_units=10,
            write_capacity_units=10,
            wait=True
        )
        yield

        if TestPrimaryDiscriminatorRangeKey.ParentModel.exists():
            TestPrimaryDiscriminatorRangeKey.ParentModel.delete_table()

    @pytest.mark.ddblocal
    def test_query_with_parent(self):
        id_ = str(uuid.uuid4())
        child_a = TestPrimaryDiscriminatorRangeKey.ChildModelA(id=id_)
        child_a.save()
        child_b = TestPrimaryDiscriminatorRangeKey.ChildModelB(id=id_)
        child_b.save()

        all_children = list(TestPrimaryDiscriminatorRangeKey.ParentModel.query(id=id_))
        assert len(all_children) == 2
        assert any(isinstance(child, TestPrimaryDiscriminatorRangeKey.ChildModelA) for child in all_children)
        assert any(isinstance(child, TestPrimaryDiscriminatorRangeKey.ChildModelB) for child in all_children)

    @pytest.mark.ddblocal
    def test_query_explicitly_with_parent(self):
        id_ = str(uuid.uuid4())
        child_a = TestPrimaryDiscriminatorRangeKey.ChildModelA(id=id_)
        child_a.save()
        child_b = TestPrimaryDiscriminatorRangeKey.ChildModelB(id=id_)
        child_b.save()

        # Query for only children who are ChildModelA.
        children_a = list(TestPrimaryDiscriminatorRangeKey.ParentModel.query(
            hash_key=id_,
            range_key_condition=TestPrimaryDiscriminatorRangeKey.ParentModel.cls == TestPrimaryDiscriminatorRangeKey.ChildModelA
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, TestPrimaryDiscriminatorRangeKey.ChildModelA) for child in children_a)

        # Query for only children who are ChildModelB.
        children_b = list(TestPrimaryDiscriminatorRangeKey.ParentModel.query(
            hash_key=id_,
            range_key_condition=TestPrimaryDiscriminatorRangeKey.ParentModel.cls == TestPrimaryDiscriminatorRangeKey.ChildModelB
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, TestPrimaryDiscriminatorRangeKey.ChildModelB) for child in children_b)

    @pytest.mark.ddblocal
    def test_query_with_child(self):
        id_ = str(uuid.uuid4())
        child_a = TestPrimaryDiscriminatorRangeKey.ChildModelA(id=id_)
        child_a.save()
        child_b = TestPrimaryDiscriminatorRangeKey.ChildModelB(id=id_)
        child_b.save()

        children_a = list(TestPrimaryDiscriminatorRangeKey.ChildModelA.query(
            hash_key=id_
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, TestPrimaryDiscriminatorRangeKey.ChildModelA) for child in children_a)
