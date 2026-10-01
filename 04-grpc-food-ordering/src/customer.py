#!/usr/bin/env python3
"""Customer CLI client for the gRPC Food Ordering system.

Usage:
    python3 src/customer.py <server_address> [customer_id]
    python3 src/customer.py localhost:50051 alice

customer_id identifies the customer to the server: only the customer who placed
an order can cancel it. Without one, a unique "guest-xxxxxx" id is generated for
this session.
"""
import sys
import threading
import uuid

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

MENU_TEXT = """
==== Customer Menu ====
1. List Restaurants
2. Place Order
3. Check Order Status
4. Track Order
5. Cancel Order
6. Exit
"""


def track_order(stub, order_id):
    try:
        for update in stub.SubscribeToOrderUpdates(pb2.OrderRequest(order_id=order_id)):
            print(f"\n[Update] Order {update.order_id} : {update.status}\n> ", end="", flush=True)
    except grpc.RpcError as e:
        print(f"\n[Tracking stopped for {order_id}] {e.details()}\n> ", end="", flush=True)


def list_restaurants(stub):
    try:
        response = stub.ListRestaurants(pb2.RestaurantRequest())
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")
        return
    for i, restaurant in enumerate(response.restaurants, start=1):
        print(f"{i}. {restaurant.name}")
        for item in restaurant.items:
            print(f"   - {item.name} : {item.price:.0f}")


def fetch_menus(stub):
    """{restaurant: {item: price}}, as the server currently offers them."""
    response = stub.ListRestaurants(pb2.RestaurantRequest())
    return {r.name: {i.name: i.price for i in r.items} for r in response.restaurants}


def place_order(stub, customer_id):
    # Entries are checked against the server's menus as they are typed; the server
    # still validates the whole order.
    try:
        menus = fetch_menus(stub)
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")
        return

    while True:
        restaurant_name = input("Restaurant name (empty to cancel): ").strip()
        if not restaurant_name:
            print("Order not placed.")
            return
        if restaurant_name in menus:
            break
        print(f"[Error] Restaurant '{restaurant_name}' does not exist. "
              f"Choose one of: {', '.join(menus)}")

    menu = menus[restaurant_name]
    items = []
    print('Enter items as "<food name>,<quantity>". Empty line to finish.')
    while True:
        line = input("item> ").strip()
        if not line:
            break
        try:
            food_name, qty = line.rsplit(",", 1)
            food_name, quantity = food_name.strip(), int(qty.strip())
        except ValueError:
            print('[Error] Invalid format, expected "<food name>,<quantity>", e.g. "Garlic Bread,2".')
            continue
        if food_name not in menu:
            print(f"[Error] '{food_name}' is not on the menu of {restaurant_name}. "
                  f"Choose from: {', '.join(menu)}")
            continue
        if quantity <= 0:
            print("[Error] Quantity must be a positive whole number.")
            continue
        items.append(pb2.OrderItem(food_name=food_name, quantity=quantity))
        print(f"  added {quantity} x {food_name}")

    if not items:
        print("No items entered, order not placed.")
        return

    try:
        response = stub.PlaceOrder(
            pb2.OrderRequest(customer_id=customer_id, restaurant_name=restaurant_name, items=items)
        )
        print("[Client] Order placed successfully.")
        print(f"[Client] Order ID: {response.order_id}")
        print(f"[Client] Total: {response.total:.0f}")
        print(f"[Client] Status: {response.status}")
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")


def check_order_status(stub):
    order_id = input("Order ID: ").strip()
    try:
        response = stub.GetOrderStatus(pb2.OrderStatusRequest(order_id=order_id))
        print(f"[Client] Order {response.order_id} ({response.restaurant_name}) : {response.status}, total {response.total:.0f}")
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")


def track_order_menu(stub):
    order_id = input("Order ID to track: ").strip()
    print(f"[Client] Tracking order {order_id}...")
    thread = threading.Thread(target=track_order, args=(stub, order_id), daemon=True)
    thread.start()


def cancel_order(stub, customer_id):
    order_id = input("Order ID to cancel: ").strip()
    try:
        response = stub.CancelOrder(pb2.CancelOrderRequest(order_id=order_id, customer_id=customer_id))
        print(f"[Client] {response.message}")
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <server_address> [customer_id]")
        sys.exit(1)

    server_address = sys.argv[1]
    customer_id = sys.argv[2] if len(sys.argv) > 2 else f"guest-{uuid.uuid4().hex[:6]}"

    channel = grpc.insecure_channel(server_address)
    stub = pb2_grpc.FoodOrderingServiceStub(channel)
    print(f"[Client] Connected to {server_address} as customer '{customer_id}'")

    try:
        menu_loop(stub, customer_id)
    except (EOFError, KeyboardInterrupt):
        print("\nGoodbye.")


def menu_loop(stub, customer_id):
    while True:
        print(MENU_TEXT)
        choice = input("> ").strip()
        if choice == "1":
            list_restaurants(stub)
        elif choice == "2":
            place_order(stub, customer_id)
        elif choice == "3":
            check_order_status(stub)
        elif choice == "4":
            track_order_menu(stub)
        elif choice == "5":
            cancel_order(stub, customer_id)
        elif choice == "6":
            print("Goodbye.")
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    main()
