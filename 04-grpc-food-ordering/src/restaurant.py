#!/usr/bin/env python3
"""Restaurant CLI client for the gRPC Food Ordering system.

Usage:
    python3 src/restaurant.py <server_address> <restaurant_name>
    python3 src/restaurant.py localhost:50051 "Pizza House"
"""
import sys

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

MENU_TEXT = """
==== Restaurant Menu ====
1. View Pending Orders
2. Accept Order
3. Start Preparing
4. Mark Ready
5. Exit
"""


def view_pending_orders(stub, restaurant_name):
    try:
        response = stub.ListOrdersForRestaurant(pb2.RestaurantOrdersRequest(restaurant_name=restaurant_name))
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")
        return
    if not response.orders:
        print("[Restaurant] No pending orders.")
        return
    for order in response.orders:
        print(f"[Restaurant] Order {order.order_id} : {order.status} (total {order.total:.0f})")


def update_status(stub, restaurant_name, new_status):
    order_id = input("Order ID: ").strip()
    try:
        response = stub.UpdateOrderStatus(
            pb2.OrderStatusUpdate(order_id=order_id, restaurant_name=restaurant_name, new_status=new_status)
        )
        print(f"[Restaurant] Order {order_id} : {new_status}")
        print(f"[Restaurant] {response.message}")
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <server_address> <restaurant_name>")
        sys.exit(1)

    server_address, restaurant_name = sys.argv[1], sys.argv[2]

    channel = grpc.insecure_channel(server_address)
    stub = pb2_grpc.FoodOrderingServiceStub(channel)

    # check the name up front, so a typo fails here and not on every update
    try:
        known = [r.name for r in stub.ListRestaurants(pb2.RestaurantRequest(), timeout=10).restaurants]
    except grpc.RpcError as e:
        print(f"[Error] Could not reach the server at {server_address}: {e.code().name}: {e.details()}")
        sys.exit(1)
    if restaurant_name not in known:
        print(f"[Error] Restaurant '{restaurant_name}' does not exist. Choose one of: {', '.join(known)}")
        sys.exit(1)

    print(f"[Restaurant] Connected as '{restaurant_name}' to {server_address}")
    try:
        menu_loop(stub, restaurant_name)
    except (EOFError, KeyboardInterrupt):
        print("\nGoodbye.")


def menu_loop(stub, restaurant_name):
    while True:
        print(MENU_TEXT)
        choice = input("> ").strip()
        if choice == "1":
            view_pending_orders(stub, restaurant_name)
        elif choice == "2":
            update_status(stub, restaurant_name, "ACCEPTED")
        elif choice == "3":
            update_status(stub, restaurant_name, "PREPARING")
        elif choice == "4":
            update_status(stub, restaurant_name, "READY")
        elif choice == "5":
            print("Goodbye.")
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    main()
