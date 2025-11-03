from assignment6_pymongo_user_service import (
    create_user, find_users, get_user_by_id, add_address, set_profile_fields
)

# Create a new user
user = create_user(
    username="Zipho",
    email="zipho@example.com",
    profile={"age": 25, "role": "student"}
)
print(f"User created with _id: {user['_id']}")

# Retrieve user by ID
user_id = user["_id"]
user = get_user_by_id(user_id)
print("User retrieved by ID:", user)

# Add an address
add_address(user_id, {"street": "123 Main St", "city": "Johannesburg"})
user = get_user_by_id(user_id)
print("User after adding address:", user)

# Update profile
set_profile_fields(user_id, {"age": 26, "role": "developer"})
user = get_user_by_id(user_id)
print("User after profile update:", user)

# Find all users
all_users = find_users()
print("All users in DB:", all_users)
