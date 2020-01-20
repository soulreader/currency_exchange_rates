def run_loop(loop):
    try:
        loop.run_forever()
    finally:
        loop.close()
