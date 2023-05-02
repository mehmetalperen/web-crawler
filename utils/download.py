import requests
import cbor
import time

from utils.response import Response

def download(url, config, logger=None):
    host, port = config.cache_server
    thisError = True
    while(thisError):
        thisError = False
        try:
            resp = requests.get(
                f"http://{host}:{port}/",
                params=[("q", f"{url}"), ("u", f"{config.user_agent}")])
        except requests.exceptions.RequestException as er:
            thisError = True
            time.sleep(4) #wait 4 seconds

    try:
        if resp and resp.content:
            return Response(cbor.loads(resp.content))
    except (EOFError, ValueError) as e:
        pass
    logger.error(f"Spacetime Response error {resp} with url {url}.")
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code,
        "url": url})
