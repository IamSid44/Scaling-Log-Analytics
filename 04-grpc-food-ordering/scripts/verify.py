#!/usr/bin/env python3
"""Correctness checks for the Food Ordering server, run against a live server.

Covers every API method, the order lifecycle, real-time updates over the
server-streaming RPC, every documented exception case,
cancel ownership, input validation, concurrent customers/restaurants, and
stream resource limits.

Usage:
    python3 src/server.py localhost:50051 &          # or 0.0.0.0:<PORT> on a cluster node
    python3 scripts/verify.py localhost:50051     # or <server_node>:<PORT> from another node

Use a freshly started server: the checks assume only the orders they place.
"""
import os
import sys
import threading
import time
from concurrent import futures

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

PASSED = 0
FAILED = 0


def check(label, condition, detail=""):
    global PASSED, FAILED
    if condition:
        PASSED += 1
        print("PASS  " + label)
    else:
        FAILED += 1
        print("FAIL  " + label + ("  -- " + detail if detail else ""))


def expect_error(label, code, fn):
    """Call fn(); pass if it raises an RpcError with the given status code."""
    try:
        fn()
    except grpc.RpcError as e:
        check("{} -> {}".format(label, code.name), e.code() == code,
              "got {}: {}".format(e.code().name, e.details()))
        return
    check("{} -> {}".format(label, code.name), False, "call succeeded")


def order(stub, restaurant, items, customer="tester"):
    return stub.PlaceOrder(pb2.OrderRequest(
        customer_id=customer,
        restaurant_name=restaurant,
        items=[pb2.OrderItem(food_name=name, quantity=qty) for name, qty in items],
    ))


def update(stub, order_id, restaurant, status):
    return stub.UpdateOrderStatus(pb2.OrderStatusUpdate(
        order_id=order_id, restaurant_name=restaurant, new_status=status))


def cancel(stub, order_id, customer="tester"):
    return stub.CancelOrder(pb2.CancelOrderRequest(order_id=order_id, customer_id=customer))


def status_of(stub, order_id):
    return stub.GetOrderStatus(pb2.OrderStatusRequest(order_id=order_id)).status


def pending_ids(stub, restaurant):
    return [o.order_id for o in stub.ListOrdersForRestaurant(
        pb2.RestaurantOrdersRequest(restaurant_name=restaurant)).orders]


def collect_updates(stub, order_id, out, timeout=15):
    """Subscribe and append every streamed status to `out` until the stream ends."""
    try:
        for u in stub.SubscribeToOrderUpdates(pb2.OrderRequest(order_id=order_id), timeout=timeout):
            out.append(u.status)
    except grpc.RpcError as e:
        out.append("ERROR:" + e.code().name)


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def main():
    if len(sys.argv) != 2:
        print("Usage: {} <server_address>".format(sys.argv[0]))
        sys.exit(2)
    channel = grpc.insecure_channel(sys.argv[1])
    grpc.channel_ready_future(channel).result(timeout=10)
    stub = pb2_grpc.FoodOrderingServiceStub(channel)

    print("== 1. Restaurant listing")
    menus = {r.name: {i.name: i.price for i in r.items}
             for r in stub.ListRestaurants(pb2.RestaurantRequest()).restaurants}
    check("two restaurants: Pizza House, Burger Point",
          set(menus) == {"Pizza House", "Burger Point"}, str(sorted(menus)))
    check("Pizza House menu and prices",
          menus.get("Pizza House") == {"Margherita Pizza": 250, "Farmhouse Pizza": 350, "Garlic Bread": 150})
    check("Burger Point menu and prices",
          menus.get("Burger Point") == {"Veg Burger": 180, "Cheese Burger": 220, "French Fries": 120})

    print("== 2. Placing an order (the example order)")
    r = order(stub, "Pizza House", [("Margherita Pizza", 1), ("Garlic Bread", 2)])
    check("total is 250*1 + 150*2 = 550", r.total == 550, str(r.total))
    check("initial status is PLACED", r.status == "PLACED", r.status)
    check("order id assigned", r.order_id != "", repr(r.order_id))
    check("GetOrderStatus returns PLACED", status_of(stub, r.order_id) == "PLACED")
    r2 = order(stub, "Pizza House", [("Farmhouse Pizza", 1)])
    check("second order gets a different id", r2.order_id != r.order_id)
    d = order(stub, "Pizza House", [("Garlic Bread", 1), ("Garlic Bread", 2)])
    check("the same item on two lines adds up (150*1 + 150*2 = 450)", d.total == 450, str(d.total))

    print("== 3. Restaurant processing + real-time updates to a subscribed customer")
    updates = []
    t = threading.Thread(target=collect_updates, args=(stub, r.order_id, updates))
    t.start()
    wait_for(lambda: len(updates) >= 1)
    pending = pending_ids(stub, "Pizza House")
    check("order appears in Pizza House's pending list", r.order_id in pending, str(pending))
    check("Pizza House orders do not appear in Burger Point's pending list",
          not set(pending) & set(pending_ids(stub, "Burger Point")))
    for s in ("ACCEPTED", "PREPARING", "READY"):
        ack = update(stub, r.order_id, "Pizza House", s)
        check("restaurant moves order to " + s, ack.success and status_of(stub, r.order_id) == s)
        time.sleep(0.2)
    t.join(timeout=10)
    check("subscriber received PLACED, ACCEPTED, PREPARING, READY in order, without polling",
          updates == ["PLACED", "ACCEPTED", "PREPARING", "READY"], str(updates))
    check("stream closes once the order is READY", not t.is_alive())
    check("READY order no longer pending", r.order_id not in pending_ids(stub, "Pizza House"))

    late = []
    collect_updates(stub, r.order_id, late, timeout=5)
    check("late subscriber to a READY order gets READY at once and the stream ends",
          late == ["READY"], str(late))

    print("== 4. Cancellation and ownership")
    c = order(stub, "Burger Point", [("Veg Burger", 2)], customer="alice")
    check("Burger Point order total 360", c.total == 360, str(c.total))
    cupdates = []
    t = threading.Thread(target=collect_updates, args=(stub, c.order_id, cupdates))
    t.start()
    wait_for(lambda: len(cupdates) >= 1)
    expect_error("another customer cancels alice's order", grpc.StatusCode.PERMISSION_DENIED,
                 lambda: cancel(stub, c.order_id, customer="bob"))
    expect_error("cancel with no customer id", grpc.StatusCode.PERMISSION_DENIED,
                 lambda: cancel(stub, c.order_id, customer=""))
    check("the refused cancels left the order PLACED", status_of(stub, c.order_id) == "PLACED")
    ack = cancel(stub, c.order_id, customer="alice")
    check("the owner can cancel a PLACED order", ack.success and status_of(stub, c.order_id) == "CANCELLED")
    t.join(timeout=10)
    check("subscriber received PLACED then CANCELLED", cupdates == ["PLACED", "CANCELLED"], str(cupdates))
    late = []
    collect_updates(stub, c.order_id, late, timeout=5)
    check("late subscriber to a CANCELLED order gets CANCELLED at once", late == ["CANCELLED"], str(late))
    check("CANCELLED order is not pending", c.order_id not in pending_ids(stub, "Burger Point"))
    expect_error("accept a CANCELLED order", grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: update(stub, c.order_id, "Burger Point", "ACCEPTED"))
    expect_error("owner cancels an already CANCELLED order", grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: cancel(stub, c.order_id, customer="alice"))

    print("== 5. Exception handling and input validation")
    expect_error("order from a non-existent restaurant", grpc.StatusCode.NOT_FOUND,
                 lambda: order(stub, "Taco Town", [("Tacos", 1)]))
    expect_error("order an unavailable food item", grpc.StatusCode.NOT_FOUND,
                 lambda: order(stub, "Pizza House", [("Veg Burger", 1)]))
    expect_error("GetOrderStatus on a non-existent order", grpc.StatusCode.NOT_FOUND,
                 lambda: status_of(stub, "O999999"))
    expect_error("GetOrderStatus with an empty order id", grpc.StatusCode.NOT_FOUND,
                 lambda: status_of(stub, ""))
    expect_error("order id typed with a zero instead of the letter O", grpc.StatusCode.NOT_FOUND,
                 lambda: status_of(stub, r.order_id.replace("O", "0")))
    expect_error("subscribe to a non-existent order", grpc.StatusCode.NOT_FOUND,
                 lambda: list(stub.SubscribeToOrderUpdates(pb2.OrderRequest(order_id="O999999"), timeout=5)))
    expect_error("cancel a non-existent order", grpc.StatusCode.NOT_FOUND,
                 lambda: cancel(stub, "O999999"))
    expect_error("update a non-existent order", grpc.StatusCode.NOT_FOUND,
                 lambda: update(stub, "O999999", "Pizza House", "ACCEPTED"))

    a = order(stub, "Pizza House", [("Garlic Bread", 1)])
    update(stub, a.order_id, "Pizza House", "ACCEPTED")
    expect_error("owner cancels an ACCEPTED order", grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: cancel(stub, a.order_id))
    expect_error("another customer cancels an ACCEPTED order (ownership is checked first)",
                 grpc.StatusCode.PERMISSION_DENIED, lambda: cancel(stub, a.order_id, customer="mallory"))
    update(stub, a.order_id, "Pizza House", "PREPARING")
    expect_error("owner cancels a PREPARING order", grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: cancel(stub, a.order_id))
    expect_error("restaurant updates another restaurant's order", grpc.StatusCode.PERMISSION_DENIED,
                 lambda: update(stub, a.order_id, "Burger Point", "READY"))
    expect_error("update claiming a restaurant that does not exist", grpc.StatusCode.PERMISSION_DENIED,
                 lambda: update(stub, a.order_id, "Taco Town", "READY"))
    expect_error("invalid transition PREPARING -> ACCEPTED (backwards)", grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: update(stub, a.order_id, "Pizza House", "ACCEPTED"))
    expect_error("unknown status value 'DELIVERED'", grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: update(stub, a.order_id, "Pizza House", "DELIVERED"))
    update(stub, a.order_id, "Pizza House", "READY")
    expect_error("invalid transition READY -> PREPARING (example case)",
                 grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: update(stub, a.order_id, "Pizza House", "PREPARING"))
    expect_error("owner cancels a READY order", grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: cancel(stub, a.order_id))
    check("failed requests left the order intact (still READY)", status_of(stub, a.order_id) == "READY")

    s = order(stub, "Pizza House", [("Garlic Bread", 1)])
    expect_error("invalid transition PLACED -> PREPARING (skipping a step)", grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: update(stub, s.order_id, "Pizza House", "PREPARING"))
    expect_error("invalid transition PLACED -> READY (skipping two steps)", grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: update(stub, s.order_id, "Pizza House", "READY"))
    expect_error("restaurant sets status to CANCELLED", grpc.StatusCode.FAILED_PRECONDITION,
                 lambda: update(stub, s.order_id, "Pizza House", "CANCELLED"))
    check("rejected transitions left the order PLACED", status_of(stub, s.order_id) == "PLACED")
    expect_error("order with no items", grpc.StatusCode.INVALID_ARGUMENT,
                 lambda: order(stub, "Pizza House", []))
    expect_error("order with quantity 0", grpc.StatusCode.INVALID_ARGUMENT,
                 lambda: order(stub, "Pizza House", [("Garlic Bread", 0)]))
    expect_error("order with a negative quantity", grpc.StatusCode.INVALID_ARGUMENT,
                 lambda: order(stub, "Pizza House", [("Garlic Bread", -2)]))
    expect_error("order with no customer id", grpc.StatusCode.INVALID_ARGUMENT,
                 lambda: order(stub, "Pizza House", [("Garlic Bread", 1)], customer=""))

    print("== 6. Concurrency")
    n_clients, per_client = 20, 10

    def customer(i):
        ids = []
        for _ in range(per_client):
            ids.append(order(stub, "Burger Point", [("French Fries", 1)], customer="c{}".format(i)).order_id)
        return ids

    with futures.ThreadPoolExecutor(max_workers=n_clients) as pool:
        all_ids = [oid for ids in pool.map(customer, range(n_clients)) for oid in ids]
    check("{} concurrent orders from {} customers all succeed".format(n_clients * per_client, n_clients),
          len(all_ids) == n_clients * per_client)
    check("every concurrent order got a unique id", len(set(all_ids)) == len(all_ids),
          "{} unique of {}".format(len(set(all_ids)), len(all_ids)))

    race = order(stub, "Pizza House", [("Margherita Pizza", 1)])
    outcomes = []

    def try_accept(_):
        try:
            update(stub, race.order_id, "Pizza House", "ACCEPTED")
            outcomes.append("ok")
        except grpc.RpcError as e:
            outcomes.append(e.code().name)

    with futures.ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(try_accept, range(10)))
    check("10 simultaneous ACCEPTs of one order: exactly one succeeds",
          outcomes.count("ok") == 1 and outcomes.count("FAILED_PRECONDITION") == 9, str(sorted(outcomes)))

    race2 = order(stub, "Pizza House", [("Margherita Pizza", 1)])
    outcomes2 = []

    def cancel_or_accept(i):
        try:
            if i % 2:
                update(stub, race2.order_id, "Pizza House", "ACCEPTED")
            else:
                cancel(stub, race2.order_id)
            outcomes2.append("ok")
        except grpc.RpcError as e:
            outcomes2.append(e.code().name)

    with futures.ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(cancel_or_accept, range(10)))
    final = status_of(stub, race2.order_id)
    check("simultaneous cancel vs accept: exactly one wins, state is consistent",
          outcomes2.count("ok") == 1 and final in ("CANCELLED", "ACCEPTED"),
          "{} final={}".format(sorted(outcomes2), final))

    m = order(stub, "Burger Point", [("Cheese Burger", 1)])
    subs = [[] for _ in range(3)]
    threads = [threading.Thread(target=collect_updates, args=(stub, m.order_id, sub)) for sub in subs]
    for th in threads:
        th.start()
    wait_for(lambda: all(len(sub) >= 1 for sub in subs))
    for st in ("ACCEPTED", "PREPARING", "READY"):
        update(stub, m.order_id, "Burger Point", st)
    for th in threads:
        th.join(timeout=10)
    check("3 subscribers to one order all receive the full update sequence",
          all(sub == ["PLACED", "ACCEPTED", "PREPARING", "READY"] for sub in subs), str(subs))

    print("== 7. Streams: many open at once, and clients that disconnect")
    n_streams = 30
    held = [order(stub, "Pizza House", [("Garlic Bread", 1)], customer="h{}".format(i)) for i in range(n_streams)]
    held_updates = [[] for _ in range(n_streams)]
    held_threads = [threading.Thread(target=collect_updates, args=(stub, o.order_id, u, 30))
                    for o, u in zip(held, held_updates)]
    for th in held_threads:
        th.start()
    all_open = wait_for(lambda: all(len(u) >= 1 for u in held_updates), timeout=10)
    check("{} tracking streams open at once".format(n_streams), all_open,
          "{} of {} opened".format(sum(1 for u in held_updates if u), n_streams))
    t0 = time.time()
    try:
        stub.ListRestaurants(pb2.RestaurantRequest(), timeout=3)
        answered = time.time() - t0
    except grpc.RpcError as e:
        answered = None
    check("server still answers other requests promptly while {} streams are open".format(n_streams),
          answered is not None and answered < 1.0,
          "no answer within 3 s" if answered is None else "took {:.2f} s".format(answered))
    for i, o in enumerate(held):
        cancel(stub, o.order_id, customer="h{}".format(i))
    for th in held_threads:
        th.join(timeout=10)
    check("all {} held streams received PLACED, CANCELLED and closed".format(n_streams),
          all(u == ["PLACED", "CANCELLED"] for u in held_updates) and not any(th.is_alive() for th in held_threads))

    g = order(stub, "Pizza House", [("Margherita Pizza", 1)])
    quitter = stub.SubscribeToOrderUpdates(pb2.OrderRequest(order_id=g.order_id), timeout=30)
    first = next(quitter)
    stayer = []
    st = threading.Thread(target=collect_updates, args=(stub, g.order_id, stayer))
    st.start()
    wait_for(lambda: len(stayer) >= 1)
    quitter.cancel()          # this client disconnects mid-stream
    time.sleep(1.5)           # the server notices within its 1 s poll
    for s2 in ("ACCEPTED", "PREPARING", "READY"):
        update(stub, g.order_id, "Pizza House", s2)
    st.join(timeout=10)
    check("a tracking client that disconnects does not disturb the other subscriber",
          first.status == "PLACED" and stayer == ["PLACED", "ACCEPTED", "PREPARING", "READY"], str(stayer))

    print("")
    print("RESULT: {} passed, {} failed".format(PASSED, FAILED))
    sys.exit(0 if FAILED == 0 else 1)


if __name__ == "__main__":
    main()
