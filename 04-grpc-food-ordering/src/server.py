#!/usr/bin/env python3
"""gRPC Food Ordering Server.

Usage:
    python3 src/server.py <host:port>        # e.g. 0.0.0.0:59676 on a cluster node
    python3 src/server.py                    # same as 0.0.0.0:50051

Every request, state change, notification, rejection and stream is logged to
stdout with a timestamp and the calling node.
"""
import logging
import queue
import socket
import sys
import threading
from concurrent import futures
from itertools import count
from urllib.parse import unquote

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

RESTAURANTS = {
    "Pizza House": {
        "Margherita Pizza": 250.0,
        "Farmhouse Pizza": 350.0,
        "Garlic Bread": 150.0,
    },
    "Burger Point": {
        "Veg Burger": 180.0,
        "Cheese Burger": 220.0,
        "French Fries": 120.0,
    },
}

PLACED, ACCEPTED, PREPARING, READY, CANCELLED = (
    "PLACED",
    "ACCEPTED",
    "PREPARING",
    "READY",
    "CANCELLED",
)

RESTAURANT_TRANSITIONS = {
    PLACED: ACCEPTED,
    ACCEPTED: PREPARING,
    PREPARING: READY,
}

PENDING_STATUSES = {PLACED, ACCEPTED, PREPARING}

# Each open SubscribeToOrderUpdates stream holds one pool thread until the
# order finishes, so the pool must be much larger than the number of streams
# expected at once, or unary requests queue behind them.
MAX_WORKER_THREADS = 64

log = logging.getLogger("server")

_host_names = {}
_host_names_lock = threading.Lock()


def caller(context):
    """The calling node's name (or address), from the connection's peer string."""
    peer = unquote(context.peer())  # e.g. "ipv4:172.16.0.12:53422"; IPv6 arrives URL-encoded
    kind, _, rest = peer.partition(":")
    ip = rest.rsplit(":", 1)[0] if kind in ("ipv4", "ipv6") else peer
    ip = ip.strip("[]")
    with _host_names_lock:
        if ip not in _host_names:
            try:
                _host_names[ip] = socket.gethostbyaddr(ip)[0].split(".")[0]
            except (OSError, UnicodeError):
                _host_names[ip] = ip
        return _host_names[ip]


def reject(context, rpc, code, message):
    """Log a rejected request, then end the RPC with a gRPC status code."""
    log.info("REJECT %s from %s: %s: %s", rpc, caller(context), code.name, message)
    context.abort(code, message)


class Order:
    def __init__(self, order_id, customer_id, restaurant_name, items, total):
        self.order_id = order_id
        self.customer_id = customer_id
        self.restaurant_name = restaurant_name
        self.items = items
        self.total = total
        self.status = PLACED
        self.subscribers = []

    def to_status_response(self):
        return pb2.OrderStatusResponse(
            order_id=self.order_id,
            restaurant_name=self.restaurant_name,
            total=self.total,
            status=self.status,
        )


class FoodOrderingServicer(pb2_grpc.FoodOrderingServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._orders = {}
        self._order_id_counter = count(101)

    def ListRestaurants(self, request, context):
        restaurants = [
            pb2.Restaurant(
                name=name,
                items=[pb2.FoodItem(name=item, price=price) for item, price in menu.items()],
            )
            for name, menu in RESTAURANTS.items()
        ]
        log.info("ListRestaurants from %s: %d restaurants", caller(context), len(restaurants))
        return pb2.RestaurantResponse(restaurants=restaurants)

    def PlaceOrder(self, request, context):
        rpc = "PlaceOrder"
        customer_id = request.customer_id.strip()
        if not customer_id:
            reject(context, rpc, grpc.StatusCode.INVALID_ARGUMENT, "A customer_id is required to place an order.")

        menu = RESTAURANTS.get(request.restaurant_name)
        if menu is None:
            reject(context, rpc, grpc.StatusCode.NOT_FOUND, f"Restaurant '{request.restaurant_name}' does not exist.")

        if not request.items:
            reject(context, rpc, grpc.StatusCode.INVALID_ARGUMENT, "An order must contain at least one item.")

        total = 0.0
        for item in request.items:
            price = menu.get(item.food_name)
            if price is None:
                reject(context, rpc, grpc.StatusCode.NOT_FOUND,
                       f"'{item.food_name}' is not available at {request.restaurant_name}.")
            if item.quantity <= 0:
                reject(context, rpc, grpc.StatusCode.INVALID_ARGUMENT, f"Invalid quantity for '{item.food_name}'.")
            total += price * item.quantity

        with self._lock:
            order_id = f"O{next(self._order_id_counter)}"
            order = Order(order_id, customer_id, request.restaurant_name, list(request.items), total)
            self._orders[order_id] = order

        items_text = ", ".join(f"{i.food_name} x{i.quantity}" for i in request.items)
        log.info("PlaceOrder from %s: %s placed by '%s' at %s: %s, total %.0f, status PLACED",
                 caller(context), order_id, customer_id, request.restaurant_name, items_text, total)
        return pb2.OrderResponse(
            order_id=order.order_id,
            restaurant_name=order.restaurant_name,
            total=order.total,
            status=order.status,
        )

    def GetOrderStatus(self, request, context):
        order = self._get_order_or_abort(request.order_id, context, "GetOrderStatus")
        with self._lock:
            response = order.to_status_response()
        log.info("GetOrderStatus from %s: %s is %s", caller(context), order.order_id, response.status)
        return response

    def SubscribeToOrderUpdates(self, request, context):
        order = self._get_order_or_abort(request.order_id, context, "SubscribeToOrderUpdates")
        who = caller(context)

        subscriber_queue = queue.Queue()
        with self._lock:
            subscriber_queue.put(order.status)
            order.subscribers.append(subscriber_queue)
            n_subscribers = len(order.subscribers)
        log.info("SubscribeToOrderUpdates from %s: stream opened for %s (currently %s, %d subscriber(s))",
                 who, order.order_id, order.status, n_subscribers)

        reason = "client disconnected"
        try:
            while context.is_active():
                try:
                    status = subscriber_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                yield pb2.OrderUpdate(order_id=order.order_id, status=status)
                log.info("  pushed %s %s to %s", order.order_id, status, who)
                if status in (READY, CANCELLED):
                    reason = f"order reached {status}"
                    return
        finally:
            with self._lock:
                if subscriber_queue in order.subscribers:
                    order.subscribers.remove(subscriber_queue)
            log.info("SubscribeToOrderUpdates: stream for %s to %s closed (%s)", order.order_id, who, reason)

    def UpdateOrderStatus(self, request, context):
        rpc = "UpdateOrderStatus"
        caller(context)  # resolve the caller's name now, not while holding the lock below
        order = self._get_order_or_abort(request.order_id, context, rpc)

        with self._lock:
            if order.restaurant_name != request.restaurant_name:
                reject(context, rpc, grpc.StatusCode.PERMISSION_DENIED,
                       f"Order {order.order_id} does not belong to '{request.restaurant_name}'.")

            expected_next = RESTAURANT_TRANSITIONS.get(order.status)
            if expected_next != request.new_status:
                reject(context, rpc, grpc.StatusCode.FAILED_PRECONDITION,
                       f"Invalid order state transition: {order.status} -> {request.new_status}.")

            previous = order.status
            order.status = request.new_status
            notified = self._notify_subscribers(order)

        log.info("UpdateOrderStatus from %s: %s %s -> %s by '%s' (notified %d subscriber(s))",
                 caller(context), order.order_id, previous, request.new_status, request.restaurant_name, notified)
        return pb2.Acknowledge(success=True, message=f"Order {order.order_id} is now {request.new_status}.")

    def CancelOrder(self, request, context):
        rpc = "CancelOrder"
        caller(context)  # resolve the caller's name now, not while holding the lock below
        order = self._get_order_or_abort(request.order_id, context, rpc)
        customer_id = request.customer_id.strip()

        with self._lock:
            if order.customer_id != customer_id:
                reject(context, rpc, grpc.StatusCode.PERMISSION_DENIED,
                       f"Order {order.order_id} was placed by another customer.")
            if order.status != PLACED:
                reject(context, rpc, grpc.StatusCode.FAILED_PRECONDITION,
                       f"Order {order.order_id} has already been {order.status.lower()} and cannot be cancelled.")
            order.status = CANCELLED
            notified = self._notify_subscribers(order)

        log.info("CancelOrder from %s: %s PLACED -> CANCELLED by '%s' (notified %d subscriber(s))",
                 caller(context), order.order_id, customer_id, notified)
        return pb2.Acknowledge(success=True, message=f"Order {order.order_id} cancelled.")

    def ListOrdersForRestaurant(self, request, context):
        with self._lock:
            pending = [
                order.to_status_response()
                for order in self._orders.values()
                if order.restaurant_name == request.restaurant_name and order.status in PENDING_STATUSES
            ]
        log.info("ListOrdersForRestaurant from %s: '%s' has %d pending order(s)",
                 caller(context), request.restaurant_name, len(pending))
        return pb2.RestaurantOrdersResponse(orders=pending)

    def _get_order_or_abort(self, order_id, context, rpc):
        with self._lock:
            order = self._orders.get(order_id)
        if order is None:
            reject(context, rpc, grpc.StatusCode.NOT_FOUND, f"Order '{order_id}' does not exist.")
        return order

    @staticmethod
    def _notify_subscribers(order):
        for subscriber_queue in order.subscribers:
            subscriber_queue.put(order.status)
        return len(order.subscribers)


def serve(address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS))
    pb2_grpc.add_FoodOrderingServiceServicer_to_server(FoodOrderingServicer(), server)
    server.add_insecure_port(address)
    server.start()
    port = address.rsplit(":", 1)[-1]
    log.info("Food Ordering gRPC server listening on %s (%d worker threads)", address, MAX_WORKER_THREADS)
    log.info("clients on other nodes should connect to %s:%s", socket.gethostname().split(".")[0], port)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("shutting down")
        server.stop(grace=1).wait()
        log.info("stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s.%(msecs)03d [Server] %(message)s", datefmt="%H:%M:%S")
    # Default to all interfaces, so clients on other nodes can connect even when no
    # address is given (as when started with plain `python3 server.py`).
    addr = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0:50051"
    serve(addr)
