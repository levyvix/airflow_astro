import yagmail


def send_email(message, subject):
    yag = yagmail.SMTP("levy.m.nunes@gmail.com", "lkcakiycoengtbps")

    yag.send("levy.vix@gmail.com", f"{subject}", contents=message)


if __name__ == "__main__":
    send_email("ola")
