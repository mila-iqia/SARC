from pydantic import BaseModel


class User(BaseModel):
    display_name: str
    email: str

    # Either we add all the ValidFields here or we add methods on the API to query the values
    # and proxies in the client code
