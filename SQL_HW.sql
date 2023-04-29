--1.

SELECT 
	 p.ProductName　,p.UnitPrice , p.CategoryID
FROM Products p
WHERE CategoryID = (SELECT CategoryID FROM products WHERE UnitPrice = (SELECT MAX(UnitPrice) FROM Products))
ORDER BY UnitPrice DESC

--2.

SELECT TOP 1
	 p.ProductName　,p.UnitPrice , p.CategoryID
FROM Products p
WHERE CategoryID = (SELECT CategoryID FROM products WHERE UnitPrice = (SELECT MAX(UnitPrice) FROM Products))
ORDER BY UnitPrice ASC 

--3.

SELECT 
	MAX(UnitPrice) - MIN(UnitPrice) AS price_diff
FROM Ｐroducts 
WHERE CategoryID  = (SELECT CategoryID FROM products WHERE UnitPrice = (SELECT MAX(UnitPrice) FROM Products))

--4.找出沒有訂過任何商品的客戶 所在的城市的所有客戶 **

SELECT *
FROM Customers
WHERE City IN
    (SELECT City
     FROM Customers
     WHERE CustomerID NOT IN
         (SELECT DISTINCT CustomerID
          FROM Orders))

-- 5. 找出第 5 貴跟第 8 便宜的產品的產品類別

--SELECT 
--	CategoryName
--FROM Categories
--WHERE CategoryID IN (
--(
--	SELECT 
--		CategoryID
--	FROM Products　
--	ORDER BY UnitPrice DESC
--	OFFSET 4 ROWS
--	FETCH NEXT 1 ROWS ONLY
--	),
--	(
--	SELECT
--		CategoryID
--	FROM Products 
--	ORDER BY UnitPrice
--	OFFSET 7 ROWS
--	FETCH NEXT 1 ROWS ONLY
--	)
--)

-- 6.找出誰買過第 5 貴跟第 8 便宜的產品

SELECT DISTINCT o.CustomerID
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
JOIN Categories c ON p.CategoryID = c.CategoryID
WHERE p.UnitPrice IN (
  SELECT UnitPrice 
  FROM (
    SELECT DISTINCT UnitPrice 
    FROM Products 
    ORDER BY UnitPrice DESC 
    OFFSET 4 ROWS 
    FETCH NEXT 2 ROWS ONLY
  ) AS t
)


-- 找出誰賣過第 5 貴跟第 8 便宜的產品
--SELECT DISTINCT o.CustomerID
--FROM Orders o
--JOIN [Order Details] od ON o.OrderID = od.OrderID
--JOIN (
--    SELECT ProductID, UnitPrice, DENSE_RANK() OVER (ORDER BY UnitPrice DESC) AS RankDesc, DENSE_RANK() OVER (ORDER BY UnitPrice ASC) AS RankAsc
--    FROM Products
--) p ON od.ProductID = p.ProductID
--WHERE p.RankDesc = 5 OR p.RankAsc = 8


-- 找出 13 號星期五的訂單 (惡魔的訂單)
SELECT *
FROM Orders
WHERE DAY(OrderDate) = 13 AND DATEPART(WEEKDAY, OrderDate) = 6


-- 找出誰訂了惡魔的訂單
SELECT DISTINCT c.CustomerID
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE DAY(o.OrderDate) = 13 AND DATEPART(WEEKDAY, o.OrderDate) = 6

-- 找出惡魔的訂單裡有什麼產品
SELECT DISTINCT p.ProductName
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
WHERE DAY(o.OrderDate) = 13 AND DATEPART(WEEKDAY, o.OrderDate) = 6

-- 列出從來沒有打折 (Discount) 出售的產品
SELECT ProductName
FROM Products
WHERE ProductID NOT IN (
  SELECT ProductID
  FROM [Order Details]
  WHERE Discount = 0
)


-- 列出購買非本國的產品的客戶
SELECT DISTINCT o.CustomerID
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
JOIN Suppliers s ON p.SupplierID = s.SupplierID
WHERE s.Country <> o.ShipCountry

-- 列出在同個城市中有公司員工可以服務的客戶
SELECT DISTINCT c.CustomerID, c.City
FROM Customers c
INNER JOIN Employees e ON c.City = e.City
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE e.EmployeeID = o.EmployeeID

-- 列出那些產品沒有人買過
SELECT ProductID, ProductName
FROM Products
WHERE NOT EXISTS (
  SELECT * FROM [Order Details] WHERE Products.ProductID = [Order Details].ProductID
)

----------------------------------------------------------------------------------------
-- 列出所有在每個月月底的訂單
SELECT *
FROM Orders
WHERE DAY(EOMONTH(OrderDate)) = DAY(OrderDate)

-- 列出每個月月底售出的產品
SELECT 
    Products.ProductName, 
    SUM([Order Details].Quantity) AS TotalQuantitySold,
    CONVERT(date, DATEADD(month, DATEDIFF(month, 0, Orders.OrderDate) + 1, -1)) AS EndOfMonth
FROM 
    [Order Details] 
    INNER JOIN Products ON [Order Details].ProductID = Products.ProductID 
    INNER JOIN Orders ON [Order Details].OrderID = Orders.OrderID 
WHERE 
    Orders.OrderDate <= CONVERT(datetime, CONVERT(varchar(7), GETDATE(), 120) + '-01') 
    AND Orders.ShippedDate IS NOT NULL
GROUP BY 
    Products.ProductName,
    CONVERT(date, DATEADD(month, DATEDIFF(month, 0, Orders.OrderDate) + 1, -1))


-- 找出有買過最貴的三個產品中的任何一個的前三個大客戶
SELECT TOP 3 Customers.CustomerID, Customers.CompanyName, SUM([Order Details].Quantity * [Order Details].UnitPrice) AS TotalSpent
FROM Customers
INNER JOIN Orders ON Customers.CustomerID = Orders.CustomerID
INNER JOIN [Order Details] ON Orders.OrderID = [Order Details].OrderID
WHERE [Order Details].ProductID IN (
  SELECT TOP 3 ProductID
  FROM Products
  WHERE Discontinued = 0
  ORDER BY UnitPrice DESC
)
GROUP BY Customers.CustomerID, Customers.CompanyName
ORDER BY TotalSpent DESC;

-- 找出有敗過銷售金額前三高個產品的前三個大客戶
--Method1
--WITH t1 AS
--(
--    SELECT
--        c.*,
--        od.*,
--        od.UnitPrice * od.Quantity * (1 - od.Discount) AS Total
--    FROM Customers c
--    INNER JOIN Orders o ON o.CustomerID = c.CustomerID
--    INNER JOIN [Order Details] od ON od.OrderID = o.OrderID
--)
--SELECT TOP 3
--    CustomerID
--FROM t1
--WHERE CustomerID IN (
--    SELECT DISTINCT
--        CustomerID
--    FROM t1
--    WHERE ProductID IN (
--        SELECT TOP 3
--            ProductID
--        FROM t1
--        GROUP BY ProductID
--        ORDER BY SUM(Total) DESC
--    )
--)
--GROUP BY CustomerID
--ORDER BY SUM(Total) DESC 

--Method2
--SELECT DISTINCT TOP 3
--    c.CustomerID, c.COmpanyName,
--    SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS SalesAmount
--FROM Customers c
--INNER JOIN Orders o ON o.CustomerID = c.CustomerID
--INNER JOIN [Order Details] od ON od.OrderID = o.OrderID
--WHERE od.ProductID IN (
--    SELECT TOP 3
--        od.ProductID
--    FROM [Order Details] od
--    GROUP BY od.ProductID
--    ORDER BY SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) DESC
--)
--GROUP BY c.CustomerID, c.CompanyName
--ORDER BY SalesAmount DESC


-- 找出有買過銷售金額前三高個產品所屬類別的前三個大客戶
SELECT TOP 3 Customers.CustomerID, Customers.CompanyName, SUM([Order Details].UnitPrice * [Order Details].Quantity * (1 - [Order Details].Discount)) AS SalesAmount
FROM [Order Details]
INNER JOIN Products ON [Order Details].ProductID = Products.ProductID
INNER JOIN Categories ON Products.CategoryID = Categories.CategoryID
INNER JOIN Orders ON [Order Details].OrderID = Orders.OrderID
INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID
WHERE Categories.CategoryID IN (
  SELECT TOP 3 Products.CategoryID
  FROM [Order Details]
  INNER JOIN Products ON [Order Details].ProductID = Products.ProductID
  GROUP BY Products.CategoryID
  ORDER BY SUM([Order Details].UnitPrice * [Order Details].Quantity * (1 - [Order Details].Discount)) DESC
)
GROUP BY Customers.CustomerID, Customers.CompanyName
ORDER BY SUM([Order Details].UnitPrice * [Order Details].Quantity * (1 - [Order Details].Discount)) DESC;


-- 列出消費總金額高於所有客戶平均消費總金額的客戶的名字，以及客戶的消費總金額
SELECT 
    c.CompanyName AS CustomerName,
    SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS TotalSpent
FROM 
    Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    JOIN [Order Details] od ON o.OrderID = od.OrderID
GROUP BY 
    c.CustomerID, c.CompanyName
HAVING 
    SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) > (
        SELECT 
            AVG(TotalSpentPerCustomer)
        FROM 
            (
                SELECT 
                    c.CustomerID,
                    SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS TotalSpentPerCustomer
                FROM 
                    Customers c
                    JOIN Orders o ON c.CustomerID = o.CustomerID
                    JOIN [Order Details] od ON o.OrderID = od.OrderID
                GROUP BY 
                    c.CustomerID
            ) AS AvgTotalSpentPerCustomer
    )


-- 列出最熱銷的產品，以及被購買的總金額
SELECT TOP 1
    p.ProductName AS '熱銷產品名稱',
    SUM(od.Quantity) AS '總銷售量',
    SUM(od.UnitPrice * od.Quantity) AS '總銷售金額'
FROM
    [Order Details] od
    JOIN Products p ON od.ProductID = p.ProductID
GROUP BY
    p.ProductName
ORDER BY
    SUM(od.Quantity) DESC;

-- 列出最少人買的產品
SELECT p.ProductName, COUNT(*) AS NumOfPurchases
FROM Products p
JOIN [Order Details] od ON p.ProductID = od.ProductID
GROUP BY p.ProductName
HAVING COUNT(*) = (
    SELECT MIN(NumOfPurchases)
    FROM (
        SELECT COUNT(*) AS NumOfPurchases
        FROM [Order Details]
        GROUP BY ProductID
    ) AS NumOfPurchasesTable
)

-- 列出最沒人要買的產品類別 (Categories)

SELECT TOP 1 Categories.CategoryName, COUNT(*) AS TotalOrders
FROM Categories
INNER JOIN Products ON Categories.CategoryID = Products.CategoryID
INNER JOIN [Order Details] ON Products.ProductID = [Order Details].ProductID
GROUP BY Categories.CategoryName
ORDER BY TotalOrders ASC;

-- 列出跟銷售最好的供應商買最多金額的客戶與購買金額 (含購買其它供應商的產品)
SELECT TOP 1 c.CustomerID, SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS TotalPurchaseAmount
FROM Orders o
JOIN [Order Details]  od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
JOIN Suppliers s ON p.SupplierID = s.SupplierID
JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE s.SupplierID IN (
  SELECT TOP 1 s2.SupplierID
  FROM Suppliers s2
  JOIN Products p2 ON s2.SupplierID = p2.SupplierID
  JOIN [Order Details]  od2 ON p2.ProductID = od2.ProductID
  GROUP BY s2.SupplierID
  ORDER BY SUM(od2.UnitPrice * od2.Quantity * (1 - od2.Discount)) DESC
)
GROUP BY c.CustomerID
ORDER BY TotalPurchaseAmount DESC;

-- 列出跟銷售最好的供應商買最多金額的客戶與購買金額 (不含購買其它供應商的產品)
SELECT TOP 1 c.CustomerID, SUM(od.UnitPrice * od.Quantity) AS TotalPurchaseAmount
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
JOIN Suppliers s ON p.SupplierID = s.SupplierID
WHERE s.SupplierID = (
    SELECT TOP 1 SupplierID
    FROM Products
    WHERE Discontinued = 0
    GROUP BY SupplierID
    ORDER BY SUM(UnitPrice * UnitsInStock) DESC
)
GROUP BY c.CustomerID
ORDER BY TotalPurchaseAmount DESC

-- 列出那些產品沒有人買過

SELECT p.ProductName
FROM Products p
LEFT JOIN [Order Details] od ON p.ProductID = od.ProductID
WHERE od.OrderID IS NULL


-- 列出沒有傳真 (Fax) 的客戶和它的消費總金額
SELECT Customers.CustomerID, Customers.CompanyName, SUM(Orders.Freight) AS TotalSpent
FROM Customers
JOIN Orders ON Customers.CustomerID = Orders.CustomerID
WHERE Customers.Fax IS NULL
GROUP BY Customers.CustomerID, Customers.CompanyName

-- 列出每一個城市消費的產品種類數量
SELECT Customers.City, COUNT(DISTINCT Products.CategoryID) AS NumCategories
FROM Customers
INNER JOIN Orders ON Customers.CustomerID = Orders.CustomerID
INNER JOIN [Order Details] ON Orders.OrderID = [Order Details].OrderID
INNER JOIN Products ON [Order Details].ProductID = Products.ProductID
GROUP BY Customers.City

-- 列出目前沒有庫存的產品在過去總共被訂購的數量
SELECT 
    Products.ProductName,
    SUM([Order Details] .Quantity) AS TotalOrdered
FROM 
    Products
    INNER JOIN [Order Details]  ON Products.ProductID = [Order Details] .ProductID
WHERE 
    Products.Discontinued = 0
    AND Products.UnitsInStock = 0
GROUP BY 
    Products.ProductName;

-- 列出目前沒有庫存的產品在過去曾經被那些客戶訂購過
SELECT DISTINCT c.CustomerID, o.OrderID
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
WHERE p.UnitsInStock = 0

-- 列出每位員工的下屬的業績總金額
SELECT 
    e.LastName + ' ' + e.FirstName AS EmployeeName,
    SUM(od.UnitPrice * od.Quantity) AS SalesTotal
FROM 
    Employees e 
INNER JOIN 
    Orders o ON e.EmployeeID = o.EmployeeID 
INNER JOIN 
    [Order Details] od ON o.OrderID = od.OrderID 
INNER JOIN 
    (SELECT 
         EmployeeID, LastName, FirstName 
     FROM 
         Employees) subordinates ON e.EmployeeID = subordinates.EmployeeID 
GROUP BY 
    e.LastName + ' ' + e.FirstName


-- 列出每家貨運公司運送最多的那一種產品類別與總數量
SELECT s.CompanyName AS Shipper, c.CategoryName AS Category, SUM(od.Quantity) AS TotalQuantity
FROM Shippers s
JOIN Orders o ON s.ShipperID = o.ShipVia
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
JOIN Categories c ON p.CategoryID = c.CategoryID
GROUP BY s.CompanyName, c.CategoryName
HAVING SUM(od.Quantity) = (
  SELECT MAX(TotalQuantity)
  FROM (
    SELECT s.CompanyName AS Shipper, c.CategoryName AS Category, SUM(od.Quantity) AS TotalQuantity
    FROM Shippers s
    JOIN Orders o ON s.ShipperID = o.ShipVia
    JOIN [Order Details] od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    JOIN Categories c ON p.CategoryID = c.CategoryID
    GROUP BY s.CompanyName, c.CategoryName
  ) AS subquery
  WHERE subquery.Shipper = s.CompanyName
)
ORDER BY Shipper

-- 列出每一個客戶買最多的產品類別與金額
SELECT c.CustomerID, c.CompanyName, p.CategoryID, SUM(od.Quantity * od.UnitPrice) AS TotalSpent
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
GROUP BY c.CustomerID, c.CompanyName, p.CategoryID
HAVING SUM(od.Quantity * od.UnitPrice) = 
    (SELECT MAX(sub.TotalSpent)
     FROM (SELECT c.CustomerID, p.CategoryID, SUM(od.Quantity * od.UnitPrice) AS TotalSpent
           FROM Customers c
           JOIN Orders o ON c.CustomerID = o.CustomerID
           JOIN [Order Details] od ON o.OrderID = od.OrderID
           JOIN Products p ON od.ProductID = p.ProductID
           GROUP BY c.CustomerID, p.CategoryID) sub
     WHERE sub.CustomerID = c.CustomerID)

-- 列出每一個客戶買最多的那一個產品與購買數量
SELECT c.CustomerID, p.ProductName, MAX(od.Quantity) as MaxQuantity
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
GROUP BY c.CustomerID, p.ProductName
HAVING MAX(od.Quantity) = (
   SELECT MAX(Quantity) as MaxQuantity
   FROM (
      SELECT c.CustomerID, p.ProductName, SUM(od.Quantity) as Quantity
      FROM Customers c
      JOIN Orders o ON c.CustomerID = o.CustomerID
      JOIN [Order Details] od ON o.OrderID = od.OrderID
      JOIN Products p ON od.ProductID = p.ProductID
      GROUP BY c.CustomerID, p.ProductName
   ) as t
   WHERE t.CustomerID = c.CustomerID
)

-- 按照城市分類，找出每一個城市最近一筆訂單的送貨時間
SELECT ShipCity, MAX(ShippedDate) AS LatestDeliveryDate
FROM Orders
GROUP BY ShipCity

-- 列出購買金額第五名與第十名的客戶，以及兩個客戶的金額差距
SELECT TOP 2
    t1.CustomerID AS CustomerID1,
    t2.CustomerID AS CustomerID2,
    ABS(t1.TotalPurchaseAmount - t2.TotalPurchaseAmount) AS PurchaseAmountDiff
FROM (
    SELECT TOP 10 
        c.CustomerID,
        SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS TotalPurchaseAmount
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    JOIN [Order Details] od ON o.OrderID = od.OrderID
    GROUP BY c.CustomerID
    ORDER BY SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) DESC
) AS t1
JOIN (
    SELECT TOP 10 
        c.CustomerID,
        SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS TotalPurchaseAmount
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    JOIN [Order Details] od ON o.OrderID = od.OrderID
    GROUP BY c.CustomerID
    ORDER BY SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) DESC
) AS t2
ON t1.CustomerID <> t2.CustomerID
ORDER BY PurchaseAmountDiff DESC;





