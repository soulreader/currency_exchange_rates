import json

import aiohttp

POST = "POST"
GET = "GET"
PUT = "PUT"
HTTP_CODE_401 = "401"


async def make_async_request(method, url, body=None, custom_header=None, *,
                             ssl_context=None, timeout=1, loop, thread_pool_executor):
    error_msg = None
    decoded_json = None
    headers = {'Content-type': 'application/json',
               'Content-Length': "{}".format(len(body) if body else 0)}
    if custom_header:
        headers.update(custom_header)
    request_timeout = aiohttp.ClientTimeout(total=timeout)
    try:
        response = None
        async with aiohttp.ClientSession(timeout=request_timeout) as session:
            if method == POST:
                response = await session.post(url, data=body, headers=headers, ssl=ssl_context)
            elif method == GET:
                response = await session.get(url, data=body, headers=headers, ssl=ssl_context)
            elif method == PUT:
                response = await session.put(url, data=body, headers=headers, ssl=ssl_context)

            if response:
                if response.status == 200 or response.status == 201 or response.status == 204:
                    response_data = await response.read()
                    if response_data:
                        response_text = await loop.run_in_executor(thread_pool_executor,
                                                                   lambda: response_data.decode('utf-8'))
                        decoded_json = await loop.run_in_executor(thread_pool_executor,
                                                                  lambda: json.loads(response_text, strict=False))
                else:
                    error_msg = "Server ({}) return error: (status: {}, reason: {!r})" \
                        .format(url, response.status, response.reason)
    except Exception as ex:
        error_msg = "Exception[{}]: {}".format(type(ex).__name__, ex)

    return error_msg, decoded_json
