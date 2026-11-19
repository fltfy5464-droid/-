"""
مقایسه روش‌های انحصار متقابل (Mutual Exclusion)

الگوریتم‌های اجباری:
1. Peterson
2. Dekker
3. Monitor
4. Semaphore
5. Readers-Writers

الگوریتم اختیاری:
6. Dining Philosophers

در هر روش، دو Thread روی یک بخش بحرانی کار می‌کنند
و یک شمارنده مشترک را افزایش می‌دهند.
اگر انحصار متقابل درست باشد، مقدار نهایی Counter باید N*2 باشد.
"""

import threading
import time
import sys


# ---------------------------------------------------------
# تنظیمات کلی
# ---------------------------------------------------------

N = 3000

# برای مشاهده بهتر جابه‌جایی Threadها
sys.setswitchinterval(1e-5)

counter = 0


# ---------------------------------------------------------
# بخش بحرانی
# ---------------------------------------------------------

def critical_section():
    """
    بخش بحرانی:
    خواندن -> تغییر -> نوشتن متغیر مشترک

    اگر دو Thread همزمان وارد این قسمت شوند،
    Race Condition ممکن است رخ دهد.
    """

    global counter

    tmp = counter

    # اجازه می‌دهیم Thread دیگری اجرا شود
    time.sleep(0)

    tmp += 1

    counter = tmp


# =========================================================
# 1) بدون قفل - فقط برای مقایسه
# =========================================================

class NoLock:

    def enter(self, i):
        pass

    def exit(self, i):
        pass


# =========================================================
# 2) Semaphore
# =========================================================

class SemaphoreLock:

    def __init__(self):
        # مقدار 1 یعنی در هر لحظه فقط یک Thread
        # می‌تواند وارد بخش بحرانی شود.
        self.sem = threading.Semaphore(1)

    def enter(self, i):
        self.sem.acquire()

    def exit(self, i):
        self.sem.release()


# =========================================================
# 3) Monitor
# =========================================================

class MonitorLock:

    def __init__(self):
        self.cond = threading.Condition()
        self.busy = False

    def enter(self, i):

        with self.cond:

            # تا زمانی که بخش بحرانی مشغول است
            # Thread منتظر می‌ماند.
            while self.busy:
                self.cond.wait()

            self.busy = True

    def exit(self, i):

        with self.cond:

            self.busy = False

            # اطلاع دادن به Threadهای منتظر
            self.cond.notify()


# =========================================================
# 4) Peterson
# =========================================================

class PetersonLock:

    def __init__(self):

        # مشخص می‌کند هر Thread تمایل دارد وارد شود یا نه
        self.flag = [False, False]

        # مشخص می‌کند نوبت کدام Thread است
        self.turn = 0

    def enter(self, i):

        j = 1 - i

        self.flag[i] = True
        self.turn = j

        # تا زمانی که Thread دیگر قصد ورود دارد
        # و نوبت اوست، منتظر می‌مانیم.
        while self.flag[j] and self.turn == j:
            time.sleep(0)

    def exit(self, i):

        self.flag[i] = False


# =========================================================
# 5) Dekker
# =========================================================

class DekkerLock:

    def __init__(self):

        self.flag = [False, False]

        # نوبت Thread
        self.turn = 0

    def enter(self, i):

        j = 1 - i

        self.flag[i] = True

        while self.flag[j]:

            if self.turn != i:

                self.flag[i] = False

                # منتظر نوبت خودمان می‌مانیم
                while self.turn != i:
                    time.sleep(0)

                self.flag[i] = True

    def exit(self, i):

        self.turn = 1 - i
        self.flag[i] = False


# =========================================================
# تابع اجرای Thread
# =========================================================

def worker(lock, thread_id):

    global counter

    for _ in range(N):

        lock.enter(thread_id)

        try:
            critical_section()

        finally:
            lock.exit(thread_id)


# =========================================================
# اجرای هر الگوریتم
# =========================================================

def run_test(name, lock):

    global counter

    counter = 0

    t1 = threading.Thread(
        target=worker,
        args=(lock, 0)
    )

    t2 = threading.Thread(
        target=worker,
        args=(lock, 1)
    )

    start = time.perf_counter()

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    end = time.perf_counter()

    expected = N * 2

    print("\n---------------------------------------")
    print("Algorithm :", name)
    print("Expected  :", expected)
    print("Result    :", counter)
    print("Time      :", round(end - start, 6), "seconds")

    if counter == expected:
        print("Status    : PASS")
    else:
        print("Status    : FAIL")


# =========================================================
# 6) Readers-Writers
# =========================================================

class ReadersWriters:

    def __init__(self):

        # تعداد خوانندگان فعال
        self.readers = 0

        # برای محافظت از متغیر readers
        self.mutex = threading.Lock()

        # فقط یک Writer می‌تواند وارد شود
        self.resource = threading.Semaphore(1)

    def reader_enter(self):

        # تغییر تعداد Readerها باید محافظت شود
        with self.mutex:

            self.readers += 1

            # اولین Reader منبع را قفل می‌کند
            if self.readers == 1:
                self.resource.acquire()

    def reader_exit(self):

        with self.mutex:

            self.readers -= 1

            # آخرین Reader منبع را آزاد می‌کند
            if self.readers == 0:
                self.resource.release()

    def writer_enter(self):

        # Writer باید دسترسی انحصاری داشته باشد
        self.resource.acquire()

    def writer_exit(self):

        self.resource.release()


def reader_task(rw, reader_id):

    for i in range(5):

        rw.reader_enter()

        print(
            f"Reader {reader_id} is reading..."
        )

        time.sleep(0.01)

        rw.reader_exit()

        time.sleep(0.01)


def writer_task(rw, writer_id):

    for i in range(5):

        rw.writer_enter()

        print(
            f"Writer {writer_id} is writing..."
        )

        time.sleep(0.02)

        rw.writer_exit()

        time.sleep(0.01)


def run_readers_writers():

    print("\n=======================================")
    print("Readers-Writers Problem")
    print("=======================================")

    rw = ReadersWriters()

    readers = [
        threading.Thread(
            target=reader_task,
            args=(rw, i)
        )
        for i in range(3)
    ]

    writers = [
        threading.Thread(
            target=writer_task,
            args=(rw, i)
        )
        for i in range(2)
    ]

    for t in readers:
        t.start()

    for t in writers:
        t.start()

    for t in readers:
        t.join()

    for t in writers:
        t.join()

    print("Readers-Writers test finished.")


# =========================================================
# 7) Dining Philosophers - اختیاری
# =========================================================

class DiningPhilosophers:

    def __init__(self, number=5):

        self.number = number

        # هر چنگال یک Lock است
        self.forks = [
            threading.Lock()
            for _ in range(number)
        ]

    def philosopher(self, philosopher_id):

        left = philosopher_id

        right = (philosopher_id + 1) % self.number

        for _ in range(3):

            print(
                f"Philosopher {philosopher_id} is thinking..."
            )

            time.sleep(0.01)

            # برای جلوگیری از Deadlock،
            # همیشه ابتدا چنگال با شماره کمتر
            # گرفته می‌شود.
            first = min(left, right)
            second = max(left, right)

            with self.forks[first]:

                with self.forks[second]:

                    print(
                        f"Philosopher {philosopher_id} is eating..."
                    )

                    time.sleep(0.01)

    def run(self):

        print("\n=======================================")
        print("Dining Philosophers Problem")
        print("=======================================")

        threads = []

        for i in range(self.number):

            t = threading.Thread(
                target=self.philosopher,
                args=(i,)
            )

            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        print("Dining Philosophers test finished.")


# =========================================================
# اجرای برنامه اصلی
# =========================================================

if __name__ == "__main__":

    print("=======================================")
    print("MUTUAL EXCLUSION COMPARISON")
    print("=======================================")

    # برای مقایسه Race Condition
    run_test("No Lock", NoLock())

    # روش‌های اجباری
    run_test("Semaphore", SemaphoreLock())

    run_test("Monitor", MonitorLock())

    run_test("Peterson", PetersonLock())

    run_test("Dekker", DekkerLock())

    # Readers-Writers
    run_readers_writers()

    # الگوریتم اختیاری
    philosophers = DiningPhilosophers()
    philosophers.run()