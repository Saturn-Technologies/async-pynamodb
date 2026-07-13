from importlib.metadata import version

import botocore.session


def _version_tuple(package_name: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version(package_name).split(".")[:3])


def _operation_input_members(service_name: str, operation_name: str) -> set[str]:
    loader = botocore.session.get_session().get_component("data_loader")
    service_model = loader.load_service_model(service_name, "service-2")
    operation = service_model["operations"][operation_name]
    input_shape = service_model["shapes"][operation["input"]["shape"]]
    return set(input_shape["members"])


def test_asyncio_extra_uses_bedrock_capable_aws_sdk() -> None:
    assert _version_tuple("aioboto3") >= (15, 5, 0)
    assert _version_tuple("aiobotocore") >= (2, 25, 1)
    assert _version_tuple("botocore") >= (1, 40, 46)

    assert {"toolConfig", "performanceConfig"} <= _operation_input_members(
        "bedrock-runtime",
        "Converse",
    )
    expected_input_members = {"outputConfiguration", "dataAutomationProfileArn"}
    assert expected_input_members <= _operation_input_members(
        "bedrock-data-automation-runtime", "InvokeDataAutomationAsync"
    )
