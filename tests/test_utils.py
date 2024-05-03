from collections import namedtuple
from unittest import TestCase

from redash import create_app
from redash.utils import (
    build_url,
    collect_parameters_from_request,
    filter_none,
    json_dumps,
    generate_token,
    render_template,
    get_cache
)


DummyRequest = namedtuple("DummyRequest", ["host", "scheme"])


class TestBuildUrl(TestCase):
    def test_simple_case(self):
        self.assertEqual(
            "http://example.com/test",
            build_url(DummyRequest("", "http"), "example.com", "/test"),
        )

    def test_uses_current_request_port(self):
        self.assertEqual(
            "http://example.com:5000/test",
            build_url(DummyRequest("example.com:5000", "http"), "example.com", "/test"),
        )

    def test_uses_current_request_schema(self):
        self.assertEqual(
            "https://example.com/test",
            build_url(DummyRequest("example.com", "https"), "example.com", "/test"),
        )

    def test_skips_port_for_default_ports(self):
        self.assertEqual(
            "https://example.com/test",
            build_url(DummyRequest("example.com:443", "https"), "example.com", "/test"),
        )
        self.assertEqual(
            "http://example.com/test",
            build_url(DummyRequest("example.com:80", "http"), "example.com", "/test"),
        )
        self.assertEqual(
            "https://example.com:80/test",
            build_url(DummyRequest("example.com:80", "https"), "example.com", "/test"),
        )
        self.assertEqual(
            "http://example.com:443/test",
            build_url(DummyRequest("example.com:443", "http"), "example.com", "/test"),
        )


class TestCollectParametersFromRequest(TestCase):
    def test_ignores_non_prefixed_values(self):
        self.assertEqual({}, collect_parameters_from_request({"test": 1}))

    def test_takes_prefixed_values(self):
        self.assertDictEqual(
            {"test": 1, "something_else": "test"},
            collect_parameters_from_request({"p_test": 1, "p_something_else": "test"}),
        )


class TestSkipNones(TestCase):
    def test_skips_nones(self):
        d = {"a": 1, "b": None}

        self.assertDictEqual(filter_none(d), {"a": 1})


class TestJsonDumps(TestCase):
    def test_handles_binary(self):
        self.assertEqual(json_dumps(memoryview(b"test")), '"74657374"')


class TestGenerateToken(TestCase):
    def test_format(self):
        token = generate_token(40)
        self.assertRegex(token, r"[a-zA-Z0-9]{40}")

class TestRenderTemplate(TestCase):
    def test_render(self):
        app = create_app()
        with app.app_context():
            d = {"failures": [{"id": 1, "name": "Failure Unit Test", "failed_at": "May 04, 2021 02:07PM UTC", "failure_reason": "", "failure_count": 1, "comment": None}]}
            html, text = [
                render_template("emails/failures.{}".format(f), d)
                for f in ["html", "txt"]
            ]
            self.assertIn('Failure Unit Test',html)
            self.assertIn('Failure Unit Test',text)

class TestGetMaxCacheTime(TestCase):
    def test_higher_max_age(self):
        # Comprueba que cuando max_age es mayor, se selecciona max_age
        self.assertEqual(get_cache(500, 300), 500)

    def test_higher_max_cache_time(self):
        # Comprueba que cuando max_cache_time es mayor, se selecciona max_cache_time
        self.assertEqual(get_cache(200, 400), 400)

    def test_equal_values(self):
        # Comprueba que cuando ambos valores son iguales, se devuelve ese valor
        self.assertEqual(get_cache(300, 300), 300)

    def test_negative_values(self):
        # Comprueba el comportamiento con valores negativos
        self.assertEqual(get_cache(-100, 300), 300)
        self.assertEqual(get_cache(300, -100), 300)

    def test_zero_values(self):
        # Comprueba el comportamiento cuando uno de los valores es cero
        self.assertEqual(get_cache(0, 300), 300)
        self.assertEqual(get_cache(300, 0), 300)

