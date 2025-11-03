from assignment6_mongoengine_user_server import create_user, find_users, get_user_by_id, update_user
from assignment6_models import Profile

# create user
profile = {"firstName": "diane", "lastName": "Ngcobo", "age": 29, "addresses": []}
u = create_user("diane", "diane@example.com", profile)
print("Created:", u)

# find users
print("Users count:", len(find_users()))

# update using raw operator
updated = update_user(u["_id"], {"$set": {"profile.age": 30}})
print("Updated:", updated)
