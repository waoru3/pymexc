import hashlib
import hmac
import json
import logging
import time
from abc import ABC
from typing import Literal, Optional, Tuple, Union
from urllib.parse import urlencode

from curl_cffi import requests

logger = logging.getLogger(__name__)

SPOT = "https://api.mexc.com"
FUTURES = "https://api.mexc.com"
WEB = "https://futures.mexc.com"


class MexcAPIError(Exception):
    pass


def futures_sign_request(
    api_key: Optional[str],
    api_secret: Optional[str],
    timestamp: str,
    method: str,
    payload: Union[dict, list, None],
) -> Tuple[str, Optional[str], Optional[dict]]:
    """Build the futures API signature per the MEXC integration guide.

    Returns (signature, body, params):
    - POST: body is the compact JSON string of payload ({} / [] / None -> ""); the
      signature is computed over api_key + timestamp + body. The caller MUST
      send body byte-identical as the request data.
    - GET/DELETE: params passes through; the signature target is the
      "&"-joined k=v pairs sorted by key.
    """
    if method == "POST":
        # {} / [] / None all mean "no parameters"; guide: "if there are no
        # parameters, use an empty string"
        body = json.dumps(payload, separators=(",", ":")) if payload else ""
        target = body
        params = None
    else:
        body = None
        params = payload or {}
        target = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    signature = hmac.new(
        (api_secret or "").encode("utf-8"),
        f"{api_key or ''}{timestamp}{target}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return signature, body, params


class OrderSide:
    BUY = "BUY"
    SELL = "SELL"


class OrderType:
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    LIMIT_MARKET = "LIMIT_MAKER"
    IMMEDIATE_OR_CANCEL = "IMMEDIATE_OR_CANCEL"
    FILL_OR_KILL = "FILL_OR_KILL"


class MexcSDK(ABC):
    """
    Initializes a new instance of the class with the given `api_key` and `api_secret` parameters.

    :param api_key: A string representing the API key.
    :param api_secret: A string representing the API secret.
    :param base_url: A string representing the base URL of the API.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str = None,
        api_secret: str = None,
        u_id: str = None,
        proxies: dict = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.u_id = u_id

        self.recvWindow = 5000

        self.base_url = base_url

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
            }
        )

        if proxies:
            self.session.proxies.update(proxies)

    @classmethod
    def sign(self, **kwargs) -> str: ...

    @classmethod
    def call(
        self,
        method: Union[Literal["GET"], Literal["POST"], Literal["PUT"], Literal["DELETE"]],
        router: str,
        *args,
        **kwargs,
    ) -> dict: ...


class _SpotHTTP(MexcSDK):
    def __init__(self, api_key: str = None, api_secret: str = None, proxies: dict = None):
        super().__init__(SPOT, api_key, api_secret, proxies=proxies)

        self.session.headers.update({"X-MEXC-APIKEY": self.api_key})

    def sign(self, query_string: str) -> str:
        """
        Generates a signature for an API request using HMAC SHA256 encryption.

        Args:
            **kwargs: Arbitrary keyword arguments representing request parameters.

        Returns:
            A hexadecimal string representing the signature of the request.
        """
        # Generate signature
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return signature

    def call(
        self,
        method: Union[Literal["GET"], Literal["POST"], Literal["PUT"], Literal["DELETE"]],
        router: str,
        auth: bool = True,
        *args,
        **kwargs,
    ) -> dict:
        if not router.startswith("/"):
            router = f"/{router}"

        # clear None values
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        if kwargs.get("params"):
            kwargs["params"] = {k: v for k, v in kwargs["params"].items() if v is not None}
        else:
            kwargs["params"] = {}

        timestamp = str(int(time.time() * 1000))
        kwargs["params"]["timestamp"] = timestamp
        kwargs["params"]["recvWindow"] = self.recvWindow

        kwargs["params"] = {k: v for k, v in sorted(kwargs["params"].items())}
        params = kwargs.pop("params")
        encoded_params = urlencode(params, doseq=True).replace("+", "%20")

        if self.api_key and self.api_secret and auth:
            params["signature"] = self.sign(encoded_params)

        response = self.session.request(method, f"{self.base_url}{router}", params=params, *args, **kwargs)

        if not response.ok:
            raise MexcAPIError(f"(code={response.json()['code']}): {response.json()['msg']}")

        return response.json()


class _FuturesHTTP(MexcSDK):
    def __init__(
        self,
        api_key: str = None,
        api_secret: str = None,
        proxies: dict = None,
        ignore_ad: bool = False,
    ):
        super().__init__(FUTURES, api_key=api_key, api_secret=api_secret, proxies=proxies)
        if not ignore_ad:
            print(
                "[pymexc] You can bypass Futures API maintance. See https://github.com/makarworld/pymexc/issues/15 for more information."
            )

        self.session.headers.update({"Content-Type": "application/json", "ApiKey": self.api_key})

    def call(
        self,
        method: Union[Literal["GET"], Literal["POST"], Literal["PUT"], Literal["DELETE"]],
        router: str,
        *args,
        **kwargs,
    ) -> dict:
        """
        Makes a request to the specified HTTP method and router.

        POST payloads (passed as either `json=` or `params=`) are serialized
        to a compact JSON body and signed over api_key + timestamp + body
        (MEXC integration guide). GET/DELETE keep query params and sign the
        sorted k=v join. Raises MexcAPIError on HTTP errors.
        """

        if not router.startswith("/"):
            router = f"/{router}"

        # Clear None values
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        # Ensure only one of 'json' or 'params' is set
        if "json" in kwargs and "params" in kwargs:
            raise ValueError("Only one of 'json' or 'params' can be specified.")

        # Clean None values inside 'json' or 'params'
        for variant in ("params", "json"):
            if kwarg_variant := kwargs.get(variant):
                if isinstance(kwarg_variant, dict):
                    kwargs[variant] = {k: v for k, v in kwarg_variant.items() if v is not None}
                # ! func cancel_order may be list
                elif isinstance(kwarg_variant, list):
                    kwargs[variant] = [v for v in kwarg_variant if v is not None]

        payload = kwargs.pop("json", None)
        if payload is None:
            payload = kwargs.pop("params", None)

        timestamp = str(int(time.time() * 1000))
        signature, body, params = futures_sign_request(self.api_key, self.api_secret, timestamp, method, payload)

        if method == "POST":
            if body:
                kwargs["data"] = body
        elif params:
            kwargs["params"] = params

        if self.api_key and self.api_secret:
            kwargs["headers"] = {
                "Request-Time": timestamp,
                "Signature": signature,
            }

        response = self.session.request(method, f"{self.base_url}{router}", *args, **kwargs)

        if not response.ok:
            try:
                error = response.json()
            except Exception:
                raise MexcAPIError(f"(HTTP {response.status_code}): {response.text[:200]}")
            raise MexcAPIError(f"(code={error.get('code')}): {error.get('message') or error.get('msg')}")

        return response.json()


class _WebHTTP(MexcSDK):
    def __init__(
        self,
        u_id: str = None,
        proxies: dict = None,
    ):
        super().__init__(WEB, u_id=u_id, proxies=proxies)
        self.session.headers.update(
            {
                "content-Type": "application/json",
                "authorization": self.u_id,
            }
        )

    def call(
        self,
        method: Union[Literal["GET"], Literal["POST"], Literal["PUT"], Literal["DELETE"]],
        router: str,
        *args,
        **kwargs,
    ) -> dict:
        """
        Makes a request to the specified HTTP method and router using the provided arguments.

        :param method: A string that represents the HTTP method(GET, POST, PUT, or DELETE) to be used.
        :type method: str
        :param router: A string that represents the API endpoint to be called.
        :type router: str
        :param *args: Variable length argument list.
        :type *args: list
        :param **kwargs: Arbitrary keyword arguments.
        :type **kwargs: dict

        :return: A dictionary containing the JSON response of the request.
        """

        if not router.startswith("/"):
            router = f"/{router}"

        # Clear None values
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        # Ensure only one of 'json' or 'params' is set
        if "json" in kwargs and "params" in kwargs:
            raise ValueError("Only one of 'json' or 'params' can be specified.")

        # Clean None values inside 'json' or 'params'
        for variant in ("params", "json"):
            if kwargs.get(variant):
                kwargs[variant] = {k: v for k, v in kwargs[variant].items() if v is not None}

        response = self.session.request(method, f"{self.base_url}{router}", *args, **kwargs)

        return response.json()
