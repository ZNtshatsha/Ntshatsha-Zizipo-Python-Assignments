from database import SessionLocal
import crud

db = SessionLocal()

# 1. Add users if table is empty
if not crud.get_all_users(db):  # only insert if no data exists
    crud.create_user(db, 'zamatlwane', 'zamatlwane@gmail.com', 25)
    crud.create_user(db, 'sinovuyo', 'sinovuyo@gmail.com', 27)
    crud.create_user(db, 'amahle', 'amahle@gmail.com', 29)
    crud.create_user(db, 'ogiyonke', 'ogiyonke@gmail.com', 23)
    print("Sample users added to the database.")

# 2. Get all users
print("\nAll users:")
for u in crud.get_all_users(db):
    print(u.username, u.email, u.age)

# 3. Find a user
print("\nFind user 'zamatlwane':")
user = crud.get_user_by_username(db, 'zamatlwane')
if user:
    print(user.username, user.email, user.age)
else:
    print("User not found.")

# 4. Add a new user
print("\nAdding new user 'Zizipo'...")
crud.create_user(db, 'Zizipo', 'Zizipo@gmail.com', 28)

# 5. Update user
print("\nUpdating Zizipo’s age to 29...")
crud.update_user_age(db, 'Zizipo', 29)
