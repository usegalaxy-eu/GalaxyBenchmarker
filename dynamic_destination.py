def dynamic_destination(user):
    username = user.username
    if username.startswith("dest_user_"):
        return username[10:]

    return "local"
