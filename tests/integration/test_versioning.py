import uuid

import pytest
from pynamodb.attributes import UnicodeAttribute, VersionAttribute
from pynamodb.exceptions import UpdateError, PutError, DoesNotExist, DeleteError
from pynamodb.models import Model


class TestVersioning:
    class TestModel(Model):
        class Meta:
            table_name = f"test-model-{uuid.uuid4()}"
            read_capacity_units = 1
            write_capacity_units = 1

        id = UnicodeAttribute(hash_key=True)
        version = VersionAttribute()
        name = UnicodeAttribute()
        nickname = UnicodeAttribute(null=True)

    @pytest.fixture(scope='class', autouse=True)
    def create_tables(self, ddb_url):
        self.TestModel.Meta.host = ddb_url
        self.TestModel.create_table(wait=True)
        yield
        self.TestModel.delete_table()

    @pytest.mark.parametrize("add_version_condition", [True, False])
    @pytest.mark.ddblocal
    def test_save_new_item(self, add_version_condition):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo")
        model.save(add_version_condition=add_version_condition)
        model.refresh()
        assert model.version == 1
        assert model.name == "Foo"

    @pytest.mark.ddblocal
    def test_save_on_conflicting_item_with_version_condition(self):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo")
        model.save()

        # This should fail because it is trying to write an existing item
        # under the same version on server-side.
        with pytest.raises(PutError) as e:
            self.TestModel(id=uid, name="Bar").save(add_version_condition=True)

        assert e.value.cause_response_code == "ConditionalCheckFailedException"

        # This should pass since it is trying to write an existing item
        # under a newer version on server-side.
        model.nickname = "Fooey"
        model.save()
        model.refresh()
        assert model.version == 2
        assert model.name == "Foo"
        assert model.nickname == "Fooey"

    @pytest.mark.ddblocal
    def test_save_on_conflicting_item_without_version_condition(self):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo")
        model.save()

        # Overwrite version 1 on server-side.
        self.TestModel(id=uid, name="Bar").save(add_version_condition=False)
        model.refresh()
        assert model.version == 1
        assert model.name == "Bar"

    @pytest.mark.ddblocal
    @pytest.mark.parametrize("add_version_condition", [True, False])
    def test_update_while_in_sync(self, add_version_condition):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo")
        model.save()
        # Do update with model that is in-sync with what's persisted in DB.
        model.update(
            actions=[self.TestModel.name.set("Bar")],
            add_version_condition=add_version_condition,
        )
        model.refresh()
        assert model.version == 2
        assert model.name == "Bar"
        assert model.nickname is None

    @pytest.mark.ddblocal
    def test_update_while_desync_with_version_condition(self):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo", version=2)
        model.save(add_version_condition=False)

        # Do update with model that is out-of-sync with what's persisted in DB.
        # This should fail since there is a version mismatch.
        with pytest.raises(UpdateError) as e:
            self.TestModel(id=uid, version=1).update(
                actions=[self.TestModel.name.set("Bar")],
                add_version_condition=True,
            )

        assert e.value.cause_response_code == "ConditionalCheckFailedException"

    @pytest.mark.ddblocal
    def test_update_while_desync_without_version_condition(self):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo", nickname="Fooey", version=2)
        model.save(add_version_condition=False)

        # Do update with model that is out-of-sync with what's persisted in DB.
        self.TestModel(id=uid, version=1).update(
            actions=[self.TestModel.nickname.set("Fuiya")],
            add_version_condition=False,
        )
        model.refresh()
        assert model.version == 4
        assert model.name == "Foo"
        assert model.nickname == "Fuiya"

    @pytest.mark.ddblocal
    def test_update_on_conflicting_upsert_with_version_condition(self):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo")
        model.save()

        with pytest.raises(UpdateError) as e:
            self.TestModel(id=uid).update(
                actions=[self.TestModel.name.set("Bar")],
                add_version_condition=True
            )

        assert e.value.cause_response_code == "ConditionalCheckFailedException"

    @pytest.mark.ddblocal
    def test_update_on_conflicting_upsert_without_version_condition(self):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo")
        model.save()

        self.TestModel(id=uid).update(
            actions=[self.TestModel.name.set("Bar")],
            add_version_condition=False
        )
        model.refresh()
        # Note the version is 2 instead of 1 version 1 (Foo) was overwritten.
        assert model.version == 2
        assert model.name == "Bar"

    @pytest.mark.ddblocal
    @pytest.mark.parametrize("add_version_condition", [True, False])
    def test_delete_while_in_sync(self, add_version_condition):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo")
        model.save()

        model.delete(add_version_condition=True)

        with pytest.raises(DoesNotExist):
            model.refresh()

    @pytest.mark.ddblocal
    def test_delete_while_desync_with_version_condition(self):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo", version=2)
        model.save(add_version_condition=False)

        with pytest.raises(DeleteError) as e:
            self.TestModel(id=uid, name="Bar", version=1).delete(add_version_condition=True)

        assert e.value.cause_response_code == "ConditionalCheckFailedException"

    @pytest.mark.ddblocal
    def test_delete_while_desync_without_version_condition(self):
        uid = str(uuid.uuid4())
        model = self.TestModel(id=uid, name="Foo", version=2)
        model.save(add_version_condition=False)

        self.TestModel(id=uid, name="Bar", version=1).delete(add_version_condition=False)

        with pytest.raises(DoesNotExist):
            model.refresh()
