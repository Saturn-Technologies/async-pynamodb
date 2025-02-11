from unittest.mock import patch

import pytest

from pynamodb.asyncio.table_connection import AsyncTableConnection
from pynamodb.connection.base import MetaTable
from pynamodb.constants import TABLE_KEY
from pynamodb.expressions.operand import Path
from tests.data import DESCRIBE_TABLE_DATA, GET_ITEM_DATA

PATCH_METHOD = 'pynamodb.asyncio.connection.AsyncConnection._make_api_call'

@pytest.mark.asyncio()
class TestAsyncTableConnection:

    @classmethod
    def setup_class(cls):
        cls.test_table_name = 'Thread'
        cls.region = 'us-east-1'

    async def test_create_connection(self):
        """
        TableConnection()
        """
        conn = AsyncTableConnection(self.test_table_name, meta_table=MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))
        assert conn

    async def test_describe_table(self):
        """
        TableConnection.describe_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn = AsyncTableConnection(self.test_table_name, meta_table=MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))
            data = await conn.describe_table()
            assert data == DESCRIBE_TABLE_DATA[TABLE_KEY]
            assert req.call_args[0][1] == {'TableName': 'Thread'}

    async def test_delete_item(self):
        """
        TableConnection.delete_item
        """
        conn = AsyncTableConnection(self.test_table_name, meta_table=MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await conn.delete_item(
                "Amazon DynamoDB",
                "How do I update multiple items?")
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'Key': {
                    'ForumName': {
                        'S': 'Amazon DynamoDB'
                    },
                    'Subject': {
                        'S': 'How do I update multiple items?'
                    }
                },
                'TableName': self.test_table_name
            }
            assert req.call_args[0][1] == params

    async def test_update_item(self):
        """
        TableConnection.update_item
        """
        conn = AsyncTableConnection(self.test_table_name, meta_table=MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await conn.update_item(
                'foo-key',
                actions=[Path('Subject').set('foo-subject')],
                range_key='foo-range-key',
            )
            params = {
                'Key': {
                    'ForumName': {
                        'S': 'foo-key'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'UpdateExpression': 'SET #0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo-subject'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'Thread'
            }
            assert req.call_args[0][1] == params

    async def test_get_item(self):
        """
        TableConnection.get_item
        """
        conn = AsyncTableConnection(self.test_table_name, meta_table=MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

        with patch(PATCH_METHOD) as req:
            req.return_value = GET_ITEM_DATA
            item = await conn.get_item("Amazon DynamoDB", "How do I update multiple items?")
            assert item == GET_ITEM_DATA

    async def test_put_item(self):
        """
        TableConnection.put_item
        """
        conn = AsyncTableConnection(self.test_table_name, meta_table=MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await conn.put_item(
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': self.test_table_name,
                'Item': {'ForumName': {'S': 'foo-value'}, 'Subject': {'S': 'foo-range-key'}}
            }
            assert req.call_args[0][1] == params

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await conn.put_item(
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'}
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'Item': {
                    'ForumName': {
                        'S': 'foo-value'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'TableName': self.test_table_name
            }
            assert req.call_args[0][1] == params

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await conn.put_item(
                'foo-key',
                range_key='foo-range-key',
                attributes={'ForumName': 'foo-value'},
                condition=Path('ForumName').does_not_exist()
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'Item': {
                    'ForumName': {
                        'S': 'foo-value'
                    },
                    'Subject': {
                        'S': 'foo-range-key'
                    }
                },
                'TableName': self.test_table_name,
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName'
                }
            }
            assert req.call_args[0][1] == params

    async def test_batch_write_item(self):
        """
        TableConnection.batch_write_item
        """
        items = []
        conn = AsyncTableConnection(self.test_table_name, meta_table=MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))
        for i in range(10):
            items.append(
                {"ForumName": "FooForum", "Subject": "thread-{}".format(i)}
            )
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await conn.batch_write_item(
                put_items=items
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    self.test_table_name: [
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-0'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-1'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-2'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-3'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-4'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-5'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-6'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-7'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-8'}}}},
                        {'PutRequest': {'Item': {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-9'}}}}
                    ]
                }
            }
            assert req.call_args[0][1] == params

    async def test_batch_get_item(self):
        """
        TableConnection.batch_get_item
        """
        items = []
        conn = AsyncTableConnection(self.test_table_name, meta_table=MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))
        for i in range(10):
            items.append(
                {"ForumName": "FooForum", "Subject": "thread-{}".format(i)}
            )

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await conn.batch_get_item(
                items
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    self.test_table_name: {
                        'Keys': [
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-0'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-1'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-2'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-3'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-4'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-5'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-6'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-7'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-8'}},
                            {'ForumName': {'S': 'FooForum'}, 'Subject': {'S': 'thread-9'}}
                        ]
                    }
                }
            }
            assert req.call_args[0][1] == params

    async def test_query(self):
        """
        TableConnection.query
        """
        conn = AsyncTableConnection(self.test_table_name, meta_table=MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await conn.query(
                "FooForum",
                Path('Subject').startswith('thread')
            )
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'KeyConditionExpression': '(#0 = :0 AND begins_with (#1, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'ForumName',
                    '#1': 'Subject'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'FooForum'
                    },
                    ':1': {
                        'S': 'thread'
                    }
                },
                'TableName': self.test_table_name
            }
            assert req.call_args[0][1] == params

    async def test_scan(self):
        """
        TableConnection.scan
        """
        conn = AsyncTableConnection(self.test_table_name, meta_table=MetaTable(DESCRIBE_TABLE_DATA[TABLE_KEY]))
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await conn.scan()
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': self.test_table_name
            }
            assert req.call_args[0][1] == params