"""Small source-shaped frames for offline EDA and Landing contract tests."""
import pandas as pd


def raw_inputs():
    address = dict(address=" 1 Road ", city=" Bangkok ", country=" Thailand ", postalcode="10100", phone="1234567890")
    frames = {
        "chinook.customer": dict(customerid=1, firstname=" First ", lastname="Last", company=None, state="Bangkok", fax=None,
                                 email=" user@example.com ", supportrepid=1, **address),
        "chinook.employee": dict(employeeid=1, firstname=" First ", lastname="Last", title="Sales", reportsto=None,
                                 hiredate="01/02/2024", state="Bangkok", fax=None, email=None, **address),
        "chinook.track": dict(trackid=1, albumid=1, genreid=1, name=" Track ", unitprice=10.0),
        "chinook.album": dict(albumid=1, title=" Album ", artistid=1),
        "chinook.artist": dict(artistid=1, name=" Artist "),
        "chinook.genre": dict(genreid=1, name=" Genre "),
        "chinook.invoice": dict(invoiceid=1, customerid=1, invoicedate="01/02/2024"),
        "chinook.invoiceline": dict(invoicelineid=1, invoiceid=1, trackid=1, unitprice=10.0, quantity=2),
        "northwind.customers": dict(customerid="A", companyname=" Company ", contactname=" Contact ", region="Bangkok", fax=None, **address),
        "northwind.employees": dict(employeeid=1, firstname=" First ", lastname="Last", title="Sales", reportsto=None,
                                    hiredate="2567-01-01", region="Bangkok", homephone="1234567890",
                                    **{key: value for key, value in address.items() if key != "phone"}),
        "northwind.products": dict(productid=1, categoryid=1, supplierid=1, productname=" Product ", unitprice=10.0),
        "northwind.categories": dict(categoryid=1, categoryname=" Category "),
        "northwind.suppliers": dict(supplierid=1, companyname=" Supplier "),
        "northwind.orders": dict(orderid=1, customerid="A", employeeid=1, orderdate="2567-01-01"),
        "northwind.order_details": dict(orderid=1, productid=1, unitprice=10.0, quantity=2, discount=0.25),
    }
    return {key: pd.DataFrame([value]) for key, value in frames.items()}
