import os.path
import sys
import pytest

import numpy as np
from io import StringIO
from ml2rt import load_model

from redis import Redis
from redis.exceptions import ResponseError
from redisplus.client import Client


tf_graph = "graph.pb"
torch_graph = "pt-minimal.pt"
dog_img = "dog.jpg"


class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio  # free up some memory
        sys.stdout = self._stdout


MODEL_DIR = os.path.dirname(os.path.abspath(__file__)) + "/testdata"
TENSOR_DIR = MODEL_DIR
script_old = r"""
def bar(a, b):
    return a + b

def bar_variadic(a, args : List[Tensor]):
    return args[0] + args[1]
"""

script = r"""
def bar(tensors: List[Tensor], keys: List[str], args: List[str]):
    a = tensors[0]
    b = tensors[1]
    return a + b

def bar_variadic(tensors: List[Tensor], keys: List[str], args: List[str]):
    a = tensors[0]
    l = tensors[1:]
    return a + l[0]
"""

script_with_redis_commands = r"""
def redis_string_int_to_tensor(redis_value: Any):
    return torch.tensor(int(str(redis_value)))

def int_set_get(tensors: List[Tensor], keys: List[str], args: List[str]):
    key = keys[0]
    value = int(args[0])
    redis.execute("SET", key, str(value))
    res = redis.execute("GET", key)
    return redis_string_int_to_tensor(res)

def func(tensors: List[Tensor], keys: List[str], args: List[str]):
    redis.execute("SET", keys[0], args[0])
    a = torch.stack(tensors).sum()
    b = redis_string_int_to_tensor(redis.execute("GET", keys[0]))
    redis.execute("DEL", keys[0])
    return b + a
"""

data_processing_script = r"""
def pre_process_3ch(tensors: List[Tensor], keys: List[str], args: List[str]):
    return tensors[0].float().div(255).unsqueeze(0)

def post_process(tensors: List[Tensor], keys: List[str], args: List[str]):
    # tf model has 1001 classes, hence negative 1
    return tensors[0].max(1)[1] - 1
"""


def get_client(debug=False):
    rc = Client(extras={"ai": {"debug": debug}})
    return rc


@pytest.fixture
def client():
    rc = Client()
    rc.flushdb()
    return rc


@pytest.mark.integrations
@pytest.mark.ai
def test_set_non_numpy_tensor(client):
    client.ai.tensorset("x", (2, 3, 4, 5), dtype="float")
    result = client.ai.tensorget("x", as_numpy=False)
    assert [2, 3, 4, 5] == result["values"]
    assert [4] == result["shape"]

    client.ai.tensorset("x", (2, 3, 4, 5), dtype="float64")
    result = client.ai.tensorget("x", as_numpy=False)
    assert [2, 3, 4, 5] == result["values"]
    assert [4] == result["shape"]
    assert "DOUBLE" == result["dtype"]

    client.ai.tensorset("x", (2, 3, 4, 5), dtype="int16", shape=(2, 2))
    result = client.ai.tensorget("x", as_numpy=False)
    assert [2, 3, 4, 5] == result["values"]
    assert [2, 2] == result["shape"]

    with pytest.raises(TypeError):
        client.ai.tensorset("x", (2, 3, 4, 5), dtype="wrongtype", shape=(2, 2))
    client.ai.tensorset("x", (2, 3, 4, 5), dtype="int8", shape=(2, 2))
    result = client.ai.tensorget("x", as_numpy=False)
    assert "INT8" == result["dtype"]
    assert [2, 3, 4, 5] == result["values"]
    assert [2, 2] == result["shape"]
    assert "values" in result

    with pytest.raises(TypeError):
        client.ai.tensorset("x")
        client.ai.tensorset(1)


@pytest.mark.integrations
@pytest.mark.ai
def test_tensorget_meta(client):
    client.ai.tensorset("x", (2, 3, 4, 5), dtype="float")
    result = client.ai.tensorget("x", meta_only=True)
    assert "values" not in result
    assert [4] == result["shape"]


@pytest.mark.integrations
@pytest.mark.ai
def test_numpy_tensor(client):
    input_array = np.array([2, 3], dtype=np.float32)
    client.ai.tensorset("x", input_array)
    values = client.ai.tensorget("x")
    assert values.dtype == np.float32

    input_array = np.array([2, 3], dtype=np.float64)
    client.ai.tensorset("x", input_array)
    values = client.ai.tensorget("x")
    assert values.dtype == np.float64

    input_array = np.array([2, 3])
    client.ai.tensorset("x", input_array)
    values = client.ai.tensorget("x")

    assert np.allclose([2, 3], values)
    assert values.dtype == np.int64
    assert values.shape == (2,)
    assert np.allclose(values, input_array)
    ret = client.ai.tensorset("x", values)
    assert ret == "OK"

    # By default tensorget returns immutable, unless as_numpy_mutable is set as True
    ret = client.ai.tensorget("x")
    with pytest.raises(ValueError):
        np.put(ret, 0, 1)
    ret = client.ai.tensorget("x", as_numpy_mutable=True)
    np.put(ret, 0, 1)
    assert ret[0] == 1

    stringarr = np.array("dummy")
    with pytest.raises(TypeError):
        client.ai.tensorset("trying", stringarr)


@pytest.mark.integrations
@pytest.mark.ai
# AI.MODELSET is deprecated by AI.MODELSTORE.
def test_deprecated_modelset(client):
    model_path = os.path.join(MODEL_DIR, "graph.pb")
    model_pb = load_model(model_path)

    with pytest.raises(ValueError):
        with pytest.deprecated_call():
            client.ai.modelset(
                "m",
                "tf",
                "wrongdevice",
                model_pb,
                inputs=["a", "b"],
                outputs=["mul"],
                tag="v1.0",
            )
    with pytest.raises(ValueError):
        with pytest.deprecated_call():
            client.ai.modelset(
                "m",
                "wrongbackend",
                "cpu",
                model_pb,
                inputs=["a", "b"],
                outputs=["mul"],
                tag="v1.0",
            )
    with pytest.deprecated_call():
        client.ai.modelset(
            "m", "tf", "cpu", model_pb, inputs=["a", "b"], outputs=["mul"], tag="v1.0"
        )
    model = client.ai.modelget("m", meta_only=True)
    assert model == {
        "backend": "TF",
        "batchsize": 0,
        "device": "cpu",
        "inputs": ["a", "b"],
        "minbatchsize": 0,
        "minbatchtimeout": 0,
        "outputs": ["mul"],
        "tag": "v1.0",
    }


@pytest.mark.integrations
@pytest.mark.ai
def test_modelstore_errors(client):
    model_path = os.path.join(MODEL_DIR, "graph.pb")
    model_pb = load_model(model_path)

    with pytest.raises(ValueError):
        client.ai.modelstore(
            None, "TF", "CPU", model_pb, inputs=["a", "b"], outputs=["mul"]
        )

    with pytest.raises(ValueError):
        client.ai.modelstore(
            "m",
            "tf",
            "wrongdevice",
            model_pb,
            inputs=["a", "b"],
            outputs=["mul"],
            tag="v1.0",
        )

    with pytest.raises(ValueError):
        client.ai.modelstore(
            "m",
            "wrongbackend",
            "cpu",
            model_pb,
            inputs=["a", "b"],
            outputs=["mul"],
            tag="v1.0",
        )

    with pytest.raises(ValueError):
        client.ai.modelstore(
            "m",
            "tf",
            "cpu",
            model_pb,
            inputs=["a", "b"],
            outputs=["mul"],
            tag="v1.0",
            minbatch=2,
        )

    with pytest.raises(ValueError):
        client.ai.modelstore(
            "m",
            "tf",
            "cpu",
            model_pb,
            inputs=["a", "b"],
            outputs=["mul"],
            tag="v1.0",
            batch=4,
            minbatchtimeout=1000,
        )

    with pytest.raises(ValueError) as e:
        client.ai.modelstore("m", "tf", "cpu", model_pb, tag="v1.0")

    with pytest.raises(ValueError) as e:
        client.ai.modelstore(
            "m",
            "torch",
            "cpu",
            model_pb,
            inputs=["a", "b"],
            outputs=["mul"],
            tag="v1.0",
        )


@pytest.mark.integrations
@pytest.mark.ai
def test_modelget_meta(client):
    model_path = os.path.join(MODEL_DIR, tf_graph)
    model_pb = load_model(model_path)

    client.ai.modelstore(
        "m", "tf", "cpu", model_pb, inputs=["a", "b"], outputs=["mul"], tag="v1.0"
    )
    model = client.ai.modelget("m", meta_only=True)
    assert model == {
        "backend": "TF",
        "batchsize": 0,
        "device": "cpu",
        "inputs": ["a", "b"],
        "minbatchsize": 0,
        "minbatchtimeout": 0,
        "outputs": ["mul"],
        "tag": "v1.0",
    }


@pytest.mark.integrations
@pytest.mark.ai
def test_modelexecute_non_list_input_output(client):
    model_path = os.path.join(MODEL_DIR, "graph.pb")
    model_pb = load_model(model_path)

    client.ai.modelstore(
        "m", "tf", "cpu", model_pb, inputs=["a", "b"], outputs=["mul"], tag="v1.7"
    )
    client.ai.tensorset("a", (2, 3), dtype="float")
    client.ai.tensorset("b", (2, 3), dtype="float")
    ret = client.ai.modelexecute("m", ["a", "b"], "out")
    assert ret == "OK"


@pytest.mark.integrations
@pytest.mark.ai
def test_nonasciichar(client):
    nonascii = "ĉ"
    model_path = os.path.join(MODEL_DIR, tf_graph)
    model_pb = load_model(model_path)
    client.ai.modelstore(
        "m" + nonascii,
        "tf",
        "cpu",
        model_pb,
        inputs=["a", "b"],
        outputs=["mul"],
        tag="v1.0",
    )
    client.ai.tensorset("a" + nonascii, (2, 3), dtype="float")
    client.ai.tensorset("b", (2, 3), dtype="float")
    client.ai.modelexecute("m" + nonascii, ["a" + nonascii, "b"], ["c" + nonascii])
    tensor = client.ai.tensorget("c" + nonascii)
    assert np.allclose(tensor, [4.0, 9.0])


@pytest.mark.integrations
@pytest.mark.ai
def test_run_tf_model(client):
    model_path = os.path.join(MODEL_DIR, tf_graph)
    bad_model_path = os.path.join(MODEL_DIR, torch_graph)

    model_pb = load_model(model_path)
    wrong_model_pb = load_model(bad_model_path)

    client.ai.modelstore(
        "m", "tf", "cpu", model_pb, inputs=["a", "b"], outputs=["mul"], tag="v1.0"
    )
    client.ai.modeldel("m")
    with pytest.raises(ResponseError):
        client.ai.modelget("m")
    client.ai.modelstore(
        "m", "tf", "cpu", model_pb, inputs=["a", "b"], outputs="mul", tag="v1.0"
    )

    # Required arguments ar None
    with pytest.raises(ValueError) as e:
        client.ai.modelexecute("m", inputs=None, outputs=None)

    # wrong model
    with pytest.raises(ResponseError) as e:
        client.ai.modelstore(
            "m", "tf", "cpu", wrong_model_pb, inputs=["a", "b"], outputs=["mul"]
        )

    client.ai.tensorset("a", (2, 3), dtype="float")
    client.ai.tensorset("b", (2, 3), dtype="float")
    client.ai.modelexecute("m", ["a", "b"], ["c"])
    tensor = client.ai.tensorget("c")
    assert np.allclose([4, 9], tensor)
    model_det = client.ai.modelget("m")
    assert model_det["backend"] == "TF"
    assert model_det["device"] == "cpu"  # TODO; RedisAI returns small letter
    assert model_det["tag"] == "v1.0"
    client.ai.modeldel("m")
    with pytest.raises(ResponseError):
        client.ai.modelget("m")


@pytest.mark.integrations
@pytest.mark.ai
# AI.SCRIPTRUN is deprecated by AI.SCRIPTEXECUTE
# and AI.SCRIPTSET is deprecated by AI.SCRIPTSTORE
def test_deprecated_scriptset_and_scriptrun(client):
    with pytest.raises(ResponseError):
        with pytest.deprecated_call():
            client.ai.scriptset("scr", "cpu", "return 1")
    with pytest.deprecated_call():
        client.ai.scriptset("scr", "cpu", script_old)
    client.ai.tensorset("a", (2, 3), dtype="float")
    client.ai.tensorset("b", (2, 3), dtype="float")

    # test bar(a, b)
    with pytest.deprecated_call():
        client.ai.scriptrun("scr", "bar", inputs=["a", "b"], outputs=["c"])
    tensor = client.ai.tensorget("c", as_numpy=False)
    assert [4, 6] == tensor["values"]

    # test bar_variadic(a, args : List[Tensor])
    with pytest.deprecated_call():
        client.ai.scriptrun(
            "scr", "bar_variadic", inputs=["a", "$", "b", "b"], outputs=["c"]
        )
    tensor = client.ai.tensorget("c", as_numpy=False)
    assert [4, 6] == tensor["values"]


@pytest.mark.integrations
@pytest.mark.ai
def test_scriptstore(client):
    # try with bad arguments:
    with pytest.raises(ValueError):
        client.ai.scriptstore("test", "cpu", script, entry_points=None)
    with pytest.raises(ValueError):
        client.ai.scriptstore("test", "cpu", script=None, entry_points="bar")
    with pytest.raises(ResponseError) as e:
        client.ai.scriptstore("test", "cpu", "return 1", "f")


@pytest.mark.integrations
@pytest.mark.ai
def test_scripts_execute(client):
    # try with bad arguments:
    with pytest.raises(ValueError):
        client.ai.scriptexecute("test", function=None, keys=None, inputs=None)
    with pytest.raises(ResponseError):
        client.ai.scriptexecute("test", "bar", inputs=["a"], outputs=["c"])

    client.ai.scriptstore("test", "cpu", script, "bar")
    client.ai.tensorset("a", (2, 3), dtype="float")
    client.ai.tensorset("b", (2, 3), dtype="float")
    client.ai.scriptexecute("test", "bar", inputs=["a", "b"], outputs=["c"])
    tensor = client.ai.tensorget("c", as_numpy=False)
    assert [4, 6] == tensor["values"]
    script_det = client.ai.scriptget("test")
    assert script_det["device"] == "cpu"
    assert script_det["source"] == script
    script_det = client.ai.scriptget("test", meta_only=True)
    assert script_det["device"] == "cpu"
    assert "source" not in script_det
    # delete the script
    client.ai.scriptdel("test")
    with pytest.raises(ResponseError):
        client.ai.scriptget("test")

    # store new script
    client.ai.scriptstore(
        "myscript{1}", "cpu", script, ["bar", "bar_variadic"], "version1"
    )
    client.ai.tensorset("a{1}", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    client.ai.tensorset("b{1}", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    client.ai.scriptexecute(
        "myscript{1}", "bar", inputs=["a{1}", "b{1}"], outputs=["c{1}"]
    )
    values = client.ai.tensorget("c{1}", as_numpy=False)
    assert np.allclose(values["values"], [4.0, 6.0, 4.0, 6.0])

    client.ai.tensorset("b1{1}", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    client.ai.scriptexecute(
        "myscript{1}",
        "bar_variadic",
        inputs=["a{1}", "b1{1}", "b{1}"],
        outputs=["c{1}"],
    )

    values = client.ai.tensorget("c{1}", as_numpy=False)["values"]
    assert values == [4.0, 6.0, 4.0, 6.0]


@pytest.mark.integrations
@pytest.mark.ai
def test_scripts_redis_commands(client):
    client.ai.scriptstore(
        "myscript{1}", "cpu", script_with_redis_commands, ["int_set_get", "func"]
    )
    client.ai.scriptexecute(
        "myscript{1}", "int_set_get", keys=["x{1}", "{1}"], args=["3"], outputs=["y{1}"]
    )
    values = client.ai.tensorget("y{1}", as_numpy=False)
    assert np.allclose(values["values"], [3])

    client.ai.tensorset("mytensor1{1}", [40], dtype="float")
    client.ai.tensorset("mytensor2{1}", [10], dtype="float")
    client.ai.tensorset("mytensor3{1}", [1], dtype="float")
    client.ai.scriptexecute(
        "myscript{1}",
        "func",
        keys=["key{1}"],
        inputs=["mytensor1{1}", "mytensor2{1}", "mytensor3{1}"],
        args=["3"],
        outputs=["my_output{1}"],
    )
    values = client.ai.tensorget("my_output{1}", as_numpy=False)
    assert np.allclose(values["values"], [54])
    assert client.get("key{1}") is None


@pytest.mark.integrations
@pytest.mark.ai
def test_run_onnxml_model(client):
    mlmodel_path = os.path.join(MODEL_DIR, "boston.onnx")
    onnxml_model = load_model(mlmodel_path)
    client.ai.modelstore("onnx_model", "onnx", "cpu", onnxml_model)
    tensor = np.ones((1, 13)).astype(np.float32)
    client.ai.tensorset("input", tensor)
    client.ai.modelexecute("onnx_model", ["input"], ["output"])
    # tests `convert_to_num`
    outtensor = client.ai.tensorget("output", as_numpy=False)
    assert int(float(outtensor["values"][0])) == 24


@pytest.mark.integrations
@pytest.mark.ai
def test_run_onnxdl_model(client):
    # A PyTorch model that finds the square
    dlmodel_path = os.path.join(MODEL_DIR, "findsquare.onnx")
    onnxdl_model = load_model(dlmodel_path)

    client.ai.modelstore("onnx_model", "onnx", "cpu", onnxdl_model)
    tensor = np.array((2,)).astype(np.float32)
    client.ai.tensorset("input", tensor)
    client.ai.modelexecute("onnx_model", ["input"], ["output"])
    outtensor = client.ai.tensorget("output")
    assert np.allclose(outtensor, [4.0])


@pytest.mark.integrations
@pytest.mark.ai
def test_run_pytorch_model(client):
    model_path = os.path.join(MODEL_DIR, torch_graph)
    ptmodel = load_model(model_path)

    client.ai.modelstore("pt_model", "torch", "cpu", ptmodel, tag="v1.0")
    client.ai.tensorset("a", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    client.ai.tensorset("b", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    client.ai.modelexecute("pt_model", ["a", "b"], ["output"])
    output = client.ai.tensorget("output", as_numpy=False)
    assert np.allclose(output["values"], [4, 6, 4, 6])


@pytest.mark.integrations
@pytest.mark.ai
def test_run_tflite_model(client):
    model_path = os.path.join(MODEL_DIR, "mnist_model_quant.tflite")
    tflmodel = load_model(model_path)
    client.ai.modelstore("tfl_model", "tflite", "cpu", tflmodel)

    input_path = os.path.join(TENSOR_DIR, "one.raw")
    with open(input_path, "rb") as f:
        img = np.frombuffer(f.read(), dtype=np.float32)
    client.ai.tensorset("img", img)
    client.ai.modelexecute("tfl_model", ["img"], ["output1", "output2"])
    output = client.ai.tensorget("output1")
    assert output == [1]


@pytest.mark.integrations
@pytest.mark.ai
# AI.MODELRUN is deprecated by AI.MODELEXECUTE
def test_deprecated_modelrun(client):
    model_path = os.path.join(MODEL_DIR, "graph.pb")
    model_pb = load_model(model_path)

    client.ai.modelstore(
        "m", "tf", "cpu", model_pb, inputs=["a", "b"], outputs=["mul"], tag="v1.0"
    )

    client.ai.tensorset("a", (2, 3), dtype="float")
    client.ai.tensorset("b", (2, 3), dtype="float")
    with pytest.deprecated_call():
        client.ai.modelrun("m", ["a", "b"], ["c"])
    tensor = client.ai.tensorget("c")
    assert np.allclose([4, 9], tensor)


@pytest.mark.integrations
@pytest.mark.ai
def test_info(client):
    model_path = os.path.join(MODEL_DIR, tf_graph)
    model_pb = load_model(model_path)
    client.ai.modelstore("m", "tf", "cpu", model_pb, inputs=["a", "b"], outputs=["mul"])
    first_info = client.ai.infoget("m")
    expected = {
        "key": "m",
        "type": "MODEL",
        "backend": "TF",
        "device": "cpu",
        "tag": "",
        "duration": 0,
        "samples": 0,
        "calls": 0,
        "errors": 0,
    }
    assert first_info == expected
    client.ai.tensorset("a", (2, 3), dtype="float")
    client.ai.tensorset("b", (2, 3), dtype="float")
    client.ai.modelexecute("m", ["a", "b"], ["c"])
    client.ai.modelexecute("m", ["a", "b"], ["c"])
    second_info = client.ai.infoget("m")
    assert second_info["calls"] == 2  # 2 model runs
    client.ai.inforeset("m")
    third_info = client.ai.infoget("m")
    # before modelrun and after reset
    assert first_info == third_info


@pytest.mark.integrations
@pytest.mark.ai
def test_model_scan(client):
    model_path = os.path.join(MODEL_DIR, tf_graph)
    model_pb = load_model(model_path)
    client.ai.modelstore(
        "m", "tf", "cpu", model_pb, inputs=["a", "b"], outputs=["mul"], tag="v1.2"
    )
    model_path = os.path.join(MODEL_DIR, "pt-minimal.pt")
    ptmodel = load_model(model_path)
    client = get_client()
    # TODO: RedisAI modelscan issue
    client.ai.modelstore("pt_model", "torch", "cpu", ptmodel)
    with pytest.warns(UserWarning):
        mlist = client.ai.modelscan()
    assert mlist == [["pt_model", ""], ["m", "v1.2"]]


@pytest.mark.integrations
@pytest.mark.ai
def test_script_scan(client):
    with pytest.deprecated_call():
        client.ai.scriptset("ket1", "cpu", script, tag="v1.0")
        client.ai.scriptset("ket2", "cpu", script)
    with pytest.warns(UserWarning):
        slist = client.ai.scriptscan()
    assert slist == [["ket1", "v1.0"], ["ket2", ""]]


@pytest.mark.integrations
@pytest.mark.ai
def test_debug(client):
    client = get_client(debug=True)
    with Capturing() as output:
        client.ai.tensorset("x", (2, 3, 4, 5), dtype="float")
    assert ["AI.TENSORSET x FLOAT 4 VALUES 2 3 4 5"] == output


# todo: connection pool
"""
@pytest.mark.integrations
@pytest.mark.ai
def test_pipeline_non_transaction(client):
    arr = np.array([[2.0, 3.0], [2.0, 3.0]], dtype=np.float32)
    pipe = client.ai.pipeline(transaction=False)
    pipe = pipe.tensorset("a", arr).set("native", 1)
    pipe = pipe.tensorget("a", as_numpy=False)
    pipe = pipe.tensorget("a", as_numpy=True).tensorget(
        "a", meta_only=True)
    result = pipe.execute()
    expected = [
        b"OK",
        True,
        {"dtype": "FLOAT", "shape": [2, 2],
            "values": [2.0, 3.0, 2.0, 3.0]},
        arr,
        {"dtype": "FLOAT", "shape": [2, 2]},
    ]
    for res, exp in zip(result, expected):
        if isinstance(res, np.ndarray):
            assert np.allclose(exp, res)
        else:
            assert res == exp


@pytest.mark.integrations
@pytest.mark.ai
def test_pipeline_transaction(client):
    arr = np.array([[2.0, 3.0], [2.0, 3.0]], dtype=np.float32)
    pipe = client.ai.pipeline(transaction=True)
    pipe = pipe.tensorset("a", arr).set("native", 1)
    pipe = pipe.tensorget("a", as_numpy=False)
    pipe = pipe.tensorget("a", as_numpy=True).tensorget(
        "a", meta_only=True)
    result = pipe.execute()
    expected = [
        b"OK",
        True,
        {"dtype": "FLOAT", "shape": [2, 2],
            "values": [2.0, 3.0, 2.0, 3.0]},
        arr,
        {"dtype": "FLOAT", "shape": [2, 2]},
    ]
    for res, exp in zip(result, expected):
        if isinstance(res, np.ndarray):
            assert np.allclose(exp, res)
        else:
            assert res == exp
"""
