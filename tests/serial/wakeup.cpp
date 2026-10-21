#include "serial_wakeup.h"
#include <poll.h>
#include <thread>
#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <vector>
#include <algorithm>
using Clock = std::chrono::steady_clock;
int main()
{
    SerialWakeup w; assert(w.init());
    assert(fcntl(w.descriptor(), F_GETFL) & O_NONBLOCK);
    assert(fcntl(w.descriptor(), F_GETFD) & FD_CLOEXEC);
    pollfd p{w.descriptor(), POLLIN, 0};
    assert(poll(&p, 1, 0) == 0);
    // Saturation must not block producers or lose the pending notification.
    for (int i=0; i<100000; i++) w.notify();
    assert(poll(&p, 1, 0) == 1); w.drain();
    assert(poll(&p, 1, 0) == 0);
    // Notifications before entering poll must remain readable.
    w.notify(); assert(poll(&p, 1, -1) == 1); w.drain();
    std::atomic<int> requested{0}, completed{0}, ready{0};
    std::atomic<bool> running{true};
    int serial[2]; assert(pipe(serial)==0);
    std::atomic<int> received{0};
    std::thread worker([&] {
        pollfd fds[2]={{serial[0],POLLIN,0},{w.descriptor(),POLLIN,0}};
        while (true) {
            w.drain();
            if (!running.load()) break;
            completed.store(requested.load());
            ready.store(1);
            int n=poll(fds,2,-1); assert(n>0);
            if (fds[0].revents & POLLIN) { char c; assert(read(serial[0],&c,1)==1); received++; }
        }
    });
    while (!ready.load()) std::this_thread::yield();
    std::vector<double> latency;
    for(int i=1;i<=1000;i++) {
        auto t=Clock::now();requested.store(i);w.notify();
        while (completed.load()<i) { assert(Clock::now()-t<std::chrono::seconds(1)); std::this_thread::yield(); }
        latency.push_back(std::chrono::duration<double,std::micro>(Clock::now()-t).count());
    }
    // Real serial readiness wakes the same blocking poll independently of TX.
    char c='x'; assert(write(serial[1],&c,1)==1);
    auto t=Clock::now();
    while (!received.load()) {assert(Clock::now()-t<std::chrono::seconds(1));std::this_thread::yield();}
    // Exercise drain/state/poll races under four concurrent producers.
    std::vector<std::thread> producers;
    for(int i=0;i<4;i++) producers.emplace_back([&]{for(int j=0;j<10000;j++){requested.fetch_add(1);w.notify();}});
    for(auto &th:producers) th.join();
    t=Clock::now();
    while(completed.load()!=requested.load()) {assert(Clock::now()-t<std::chrono::seconds(1));std::this_thread::yield();}
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    t=Clock::now();running.store(false);w.notify();worker.join();
    assert(Clock::now()-t<std::chrono::seconds(1));
    close(serial[0]);close(serial[1]);
    std::sort(latency.begin(),latency.end());
    std::cout<<"PASS: saturation, pre-poll notification, RX readiness, 40000 concurrent notifications, idle shutdown\n"
             <<"TX wake-to-observation latency us: median="<<latency[500]<<" p95="<<latency[950]<<" max="<<latency.back()<<"\n";
}
