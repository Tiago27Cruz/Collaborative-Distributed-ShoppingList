# SDLE Second Assignment

SDLE Second Assignment of group T03G15.

Group members:

1. João Lourenço (up202108863@up.pt)
2. Tiago Cruz (up202108810@up.pt)
3. Tomás Xavier (up202108759@up.pt)

## Video Demonstration
https://youtu.be/yIzf2fruHSs

## Run

### Broker

```sh
python Broker.py
```

### Workers

```sh
python Worker.py server1 # Run this command as many times as needed, changing the server number
```

### Client

```sh
python Client.py create user1 add banana 2 remove apple 3 remove cocacola 3 delete banana
python Client.py use 5374fe69a65bdfdf144ddbc1e6e68672 user2 add banana 3 remove bandaid 1
python Client.py view e2a2d7d6f97c94feb12b183393e690da
python Client.py delete 20c1cb9c240220aa14815c8b5e2b4aff
```
