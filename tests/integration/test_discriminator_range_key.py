import uuid

import pytest
from unittest.mock import patch

from pynamodb.attributes import DiscriminatorRangeKeyAttribute, UnicodeAttribute, DiscriminatorAttribute
from pynamodb.expressions.condition import Condition, Comparison
from pynamodb.exceptions import GetError
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex
from pynamodb.models import Model
from pynamodb.pagination import ResultIterator


class PrimaryDiscriminatorRangeKeyParentModel(Model):
    """
    Test model that uses a discriminator as the range key in the primary composite key.
    """

    class Meta:
        table_name = 'primary_discriminator_range_key_table'
        read_capacity_units = 10
        write_capacity_units = 10

    id = UnicodeAttribute(hash_key=True)
    cls = DiscriminatorAttribute(range_key=True)


class PrimaryDiscriminatorRangeKeyChildModelA(PrimaryDiscriminatorRangeKeyParentModel, discriminator='child_a'):
    pass


class PrimaryDiscriminatorRangeKeyChildModelB(PrimaryDiscriminatorRangeKeyParentModel, discriminator='child_b'):
    pass


class DiscriminatorRangeKeyIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'discriminator_range_key_index'
        projection = AllProjection()
        read_capacity_units = 10
        write_capacity_units = 10

    id = UnicodeAttribute(hash_key=True)
    cls = DiscriminatorAttribute(range_key=True)


class SecondaryDiscriminatorRangeKeyParentModel(Model):
    """
    Test model that uses a discriminator as the range key in a secondary index.
    """

    class Meta:
        table_name = 'secondary_discriminator_range_key_table'
        read_capacity_units = 10
        write_capacity_units = 10

    id = UnicodeAttribute(hash_key=True)
    other_id = UnicodeAttribute(range_key=True)
    cls = DiscriminatorAttribute()

    index = DiscriminatorRangeKeyIndex()


class SecondaryDiscriminatorRangeKeyChildModelA(SecondaryDiscriminatorRangeKeyParentModel, discriminator='child_a'):
    pass


class SecondaryDiscriminatorRangeKeyChildModelB(SecondaryDiscriminatorRangeKeyParentModel, discriminator='child_b'):
    pass


DDB_TABLES = [
    PrimaryDiscriminatorRangeKeyParentModel,
    SecondaryDiscriminatorRangeKeyParentModel
]


@pytest.fixture(scope='module', autouse=True)
def create_tables(ddb_url):
    for table in DDB_TABLES:
        table.Meta.host = ddb_url
        table.create_table(wait=True)

    yield

    for table in DDB_TABLES:
        if table.exists():
            table.delete_table()


class TestDiscriminator:
    """
    Test cases where there is a discriminator but it is not part of the composite key.
    """

    @pytest.mark.ddblocal
    def test_query_with_parent(self):
        id_ = str(uuid.uuid4())
        child_a = SecondaryDiscriminatorRangeKeyChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_a.save()
        child_b = SecondaryDiscriminatorRangeKeyChildModelB(id=id_, other_id=str(uuid.uuid4()))
        child_b.save()

        # Query for both ChildModelA and ChildModelB entries, and use a key covering both;
        # nothing should be filtered out.
        all_children = list(SecondaryDiscriminatorRangeKeyParentModel.query(hash_key=id_))
        assert len(all_children) == 2
        assert any(isinstance(child, SecondaryDiscriminatorRangeKeyChildModelA) for child in all_children)
        assert any(isinstance(child, SecondaryDiscriminatorRangeKeyChildModelB) for child in all_children)

    @pytest.mark.ddblocal
    def test_query_with_child(self):
        id_ = str(uuid.uuid4())
        child_a = SecondaryDiscriminatorRangeKeyChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_a.save()
        child_b = SecondaryDiscriminatorRangeKeyChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_b.save()

        # Query for ChildModelA entries and use a key covering only ChildModelA entries.
        children_a = list(SecondaryDiscriminatorRangeKeyChildModelA.query(
            hash_key=id_, range_key_condition=SecondaryDiscriminatorRangeKeyChildModelA.other_id == child_a.other_id
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, SecondaryDiscriminatorRangeKeyChildModelA) for child in children_a)


    @pytest.mark.ddblocal
    def test_query_implicit_filter(self):
        id_ = str(uuid.uuid4())
        child_a = SecondaryDiscriminatorRangeKeyChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_a.save()
        child_b = SecondaryDiscriminatorRangeKeyChildModelB(id=id_, other_id=str(uuid.uuid4()))
        child_b.save()

        # Query for ChildModelA entries but use a key covering both ChildModelA and ChildModelB entries;
        # the latter should be implcitly filtered out.
        children_a = list(SecondaryDiscriminatorRangeKeyChildModelA.query(
            hash_key=id_
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, SecondaryDiscriminatorRangeKeyChildModelA) for child in children_a)

        # Query for ChildModelB entries but use a key covering only ChildModelA entries,
        # which should be implcitly filtered out.
        children_b = list(SecondaryDiscriminatorRangeKeyChildModelB.query(
            hash_key=id_, range_key_condition=SecondaryDiscriminatorRangeKeyParentModel.other_id == child_a.other_id
        ))
        assert len(children_b) == 0


class TestPrimaryDiscriminatorRangeKey:
    """
    Test cases where there is a discriminator and it is the primary range key.
    """

    @pytest.mark.ddblocal
    def test_query_with_parent(self):
        id_ = str(uuid.uuid4())
        child_a = PrimaryDiscriminatorRangeKeyChildModelA(id=id_)
        child_a.save()
        child_b = PrimaryDiscriminatorRangeKeyChildModelB(id=id_)
        child_b.save()

        # Query for both ChildModelA and ChildModelB entries, and use a key covering both;
        # nothing should be filtered out.
        all_children = list(PrimaryDiscriminatorRangeKeyParentModel.query(hash_key=id_))
        assert len(all_children) == 2
        assert any(isinstance(child, PrimaryDiscriminatorRangeKeyChildModelA) for child in all_children)
        assert any(isinstance(child, PrimaryDiscriminatorRangeKeyChildModelB) for child in all_children)

    @pytest.mark.ddblocal
    def test_query_explicitly_with_parent(self):
        id_ = str(uuid.uuid4())
        child_a = PrimaryDiscriminatorRangeKeyChildModelA(id=id_)
        child_a.save()
        child_b = PrimaryDiscriminatorRangeKeyChildModelB(id=id_)
        child_b.save()

        # Query for both ChildModelA and ChildModelB entries, but use a key covering only ChildModelA entries.
        children_a = list(PrimaryDiscriminatorRangeKeyParentModel.query(
            hash_key=id_,
            range_key_condition=PrimaryDiscriminatorRangeKeyParentModel.cls == PrimaryDiscriminatorRangeKeyChildModelA
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, PrimaryDiscriminatorRangeKeyChildModelA) for child in children_a)

        # Query for both ChildModelA and ChildModelB entries, but use a key covering only ChildModelB entries.
        children_b = list(PrimaryDiscriminatorRangeKeyParentModel.query(
            hash_key=id_,
            range_key_condition=PrimaryDiscriminatorRangeKeyParentModel.cls == PrimaryDiscriminatorRangeKeyChildModelB
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, PrimaryDiscriminatorRangeKeyChildModelB) for child in children_b)

    @pytest.mark.ddblocal
    def test_query_with_child(self):
        id_ = str(uuid.uuid4())
        child_a = PrimaryDiscriminatorRangeKeyChildModelA(id=id_)
        child_a.save()
        child_b = PrimaryDiscriminatorRangeKeyChildModelB(id=id_)
        child_b.save()

        # Query for ChildModelA entries, but use a key covering both ChildModelA and ChildModelB entries;
        # the range key should have implicitly targeted only ChildModelA entries.
        with patch.object(target=ResultIterator, attribute='__init__', return_value=None) as mock_result_it:
            PrimaryDiscriminatorRangeKeyChildModelA.query(hash_key=id_)
            actual_range_key_condition = mock_result_it.call_args.args[2]['range_key_condition']
        expected_range_key_condition = "cls = {'S': 'child_a'}"
        assert str(actual_range_key_condition) == expected_range_key_condition

        children_a = list(PrimaryDiscriminatorRangeKeyChildModelA.query(hash_key=id_))
        assert len(children_a) == 1
        assert all(isinstance(child, PrimaryDiscriminatorRangeKeyChildModelA) for child in children_a)

    def test_get_with_parent(self):
        id_ = str(uuid.uuid4())
        child_a = PrimaryDiscriminatorRangeKeyChildModelA(id=id_)
        child_a.save()
        child_b = PrimaryDiscriminatorRangeKeyChildModelB(id=id_)
        child_b.save()

        with pytest.raises(GetError):
            PrimaryDiscriminatorRangeKeyParentModel.get(id_)

    def test_get_explicitly_with_parent(self):
        id_ = str(uuid.uuid4())
        child_a = PrimaryDiscriminatorRangeKeyChildModelA(id=id_)
        child_a.save()
        child_b = PrimaryDiscriminatorRangeKeyChildModelB(id=id_)
        child_b.save()

        child_a_result = PrimaryDiscriminatorRangeKeyParentModel.get(
            hash_key=id_, range_key=PrimaryDiscriminatorRangeKeyChildModelA
        )
        assert isinstance(child_a_result, PrimaryDiscriminatorRangeKeyChildModelA)

        child_b_result = PrimaryDiscriminatorRangeKeyParentModel.get(
            hash_key=id_, range_key=PrimaryDiscriminatorRangeKeyChildModelB
        )
        assert isinstance(child_b_result, PrimaryDiscriminatorRangeKeyChildModelB)

    def test_get_with_child(self):
        id_ = str(uuid.uuid4())
        child_a = PrimaryDiscriminatorRangeKeyChildModelA(id=id_)
        child_a.save()
        child_b = PrimaryDiscriminatorRangeKeyChildModelB(id=id_)
        child_b.save()

        child_a_result = PrimaryDiscriminatorRangeKeyChildModelA.get(id_)
        assert isinstance(child_a_result, PrimaryDiscriminatorRangeKeyChildModelA)

        child_b_result = PrimaryDiscriminatorRangeKeyChildModelB.get(id_)
        assert isinstance(child_b_result, PrimaryDiscriminatorRangeKeyChildModelB)


class TestSecondaryDiscriminatorRangeKey:
    """
    Test cases where there is a discriminator and it is the secondary range key (range key in a GSI/LSI).
    """

    @pytest.mark.ddblocal
    def test_query_index_with_parent(self):
        id_ = str(uuid.uuid4())
        child_a = SecondaryDiscriminatorRangeKeyChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_a.save()
        child_b = SecondaryDiscriminatorRangeKeyChildModelB(id=id_, other_id=str(uuid.uuid4()))
        child_b.save()

        # Query for both ChildModelA and ChildModelB entries, and use a key covering both;
        # nothing should be filtered out.
        all_children = list(SecondaryDiscriminatorRangeKeyParentModel.query(
            hash_key=id_,
            index_name=SecondaryDiscriminatorRangeKeyParentModel.index.Meta.index_name
        ))
        assert len(all_children) == 2
        assert any(isinstance(child, SecondaryDiscriminatorRangeKeyChildModelA) for child in all_children)
        assert any(isinstance(child, SecondaryDiscriminatorRangeKeyChildModelB) for child in all_children)

    @pytest.mark.ddblocal
    def test_query_explicitly_with_parent(self):
        id_ = str(uuid.uuid4())
        child_a = SecondaryDiscriminatorRangeKeyChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_a.save()
        child_b = SecondaryDiscriminatorRangeKeyChildModelB(id=id_, other_id=str(uuid.uuid4()))
        child_b.save()

        # Query for both ChildModelA and ChildModelB entries, but use a key covering only ChildModelA entries.
        children_a = list(SecondaryDiscriminatorRangeKeyParentModel.query(
            hash_key=id_,
            range_key_condition=SecondaryDiscriminatorRangeKeyParentModel.cls == SecondaryDiscriminatorRangeKeyChildModelA,
            index_name=SecondaryDiscriminatorRangeKeyParentModel.index.Meta.index_name
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, SecondaryDiscriminatorRangeKeyChildModelA) for child in children_a)

        # Query for both ChildModelA and ChildModelB entries, but use a key covering only ChildModelB entries.
        children_b = list(SecondaryDiscriminatorRangeKeyParentModel.query(
            hash_key=id_,
            range_key_condition=SecondaryDiscriminatorRangeKeyParentModel.cls == SecondaryDiscriminatorRangeKeyChildModelB,
            index_name=SecondaryDiscriminatorRangeKeyParentModel.index.Meta.index_name
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, SecondaryDiscriminatorRangeKeyChildModelB) for child in children_b)

    @pytest.mark.ddblocal
    def test_query_with_child(self):
        id_ = str(uuid.uuid4())
        child_a = SecondaryDiscriminatorRangeKeyChildModelA(id=id_, other_id=str(uuid.uuid4()))
        child_a.save()
        child_b = SecondaryDiscriminatorRangeKeyChildModelB(id=id_, other_id=str(uuid.uuid4()))
        child_b.save()

        # Query for ChildModelA entries, but use a key covering both ChildModelA and ChildModelB entries;
        # the range key should have implicitly targeted only ChildModelA entries.
        with patch.object(target=ResultIterator, attribute='__init__', return_value=None) as mock_result_it:
            SecondaryDiscriminatorRangeKeyChildModelA.query(
                hash_key=id_,
                index_name=SecondaryDiscriminatorRangeKeyParentModel.index.Meta.index_name
            )
            actual_range_key_condition = mock_result_it.call_args.args[2]['range_key_condition']
        expected_range_key_condition = "cls = {'S': 'child_a'}"
        assert str(actual_range_key_condition) == expected_range_key_condition

        children_a = list(SecondaryDiscriminatorRangeKeyChildModelA.query(
            hash_key=id_,
            index_name=SecondaryDiscriminatorRangeKeyParentModel.index.Meta.index_name
        ))
        assert len(children_a) == 1
        assert all(isinstance(child, SecondaryDiscriminatorRangeKeyChildModelA) for child in children_a)
