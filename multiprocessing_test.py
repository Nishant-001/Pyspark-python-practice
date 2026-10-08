import multiprocessing
import sys

def child():
    print("CHILD OK")
    print("Child Python:", sys.executable)

if __name__ == "__main__":
    print("Parent Python:", sys.executable)

    p = multiprocessing.Process(target=child)
    p.start()
    p.join()

    print("Child exit code:", p.exitcode)
