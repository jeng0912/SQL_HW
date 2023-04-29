--1.

SELECT 
	 p.ProductName�@,p.UnitPrice , p.CategoryID
FROM Products p
WHERE CategoryID = (SELECT CategoryID FROM products WHERE UnitPrice = (SELECT MAX(UnitPrice) FROM Products))
ORDER BY UnitPrice DESC

--2.

SELECT TOP 1
	 p.ProductName�@,p.UnitPrice , p.CategoryID
FROM Products p
WHERE CategoryID = (SELECT CategoryID FROM products WHERE UnitPrice = (SELECT MAX(UnitPrice) FROM Products))
ORDER BY UnitPrice ASC 

--3.

SELECT 
	MAX(UnitPrice) - MIN(UnitPrice) AS price_diff
FROM ��roducts 
WHERE CategoryID  = (SELECT CategoryID FROM products WHERE UnitPrice = (SELECT MAX(UnitPrice) FROM Products))

--4.��X�S���q�L����ӫ~���Ȥ� �Ҧb���������Ҧ��Ȥ� **

SELECT *
FROM Customers
WHERE City IN
    (SELECT City
     FROM Customers
     WHERE CustomerID NOT IN
         (SELECT DISTINCT CustomerID
          FROM Orders))

-- 5. ��X�� 5 �Q��� 8 �K�y�����~�����~���O

--SELECT 
--	CategoryName
--FROM Categories
--WHERE CategoryID IN (
--(
--	SELECT 
--		CategoryID
--	FROM Products�@
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

-- 6.��X�ֶR�L�� 5 �Q��� 8 �K�y�����~

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


-- ��X�ֽ�L�� 5 �Q��� 8 �K�y�����~
--SELECT DISTINCT o.CustomerID
--FROM Orders o
--JOIN [Order Details] od ON o.OrderID = od.OrderID
--JOIN (
--    SELECT ProductID, UnitPrice, DENSE_RANK() OVER (ORDER BY UnitPrice DESC) AS RankDesc, DENSE_RANK() OVER (ORDER BY UnitPrice ASC) AS RankAsc
--    FROM Products
--) p ON od.ProductID = p.ProductID
--WHERE p.RankDesc = 5 OR p.RankAsc = 8


-- ��X 13 ���P�������q�� (�c�]���q��)
SELECT *
FROM Orders
WHERE DAY(OrderDate) = 13 AND DATEPART(WEEKDAY, OrderDate) = 6


-- ��X�֭q�F�c�]���q��
SELECT DISTINCT c.CustomerID
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE DAY(o.OrderDate) = 13 AND DATEPART(WEEKDAY, o.OrderDate) = 6

-- ��X�c�]���q��̦����򲣫~
SELECT DISTINCT p.ProductName
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
WHERE DAY(o.OrderDate) = 13 AND DATEPART(WEEKDAY, o.OrderDate) = 6

-- �C�X�q�ӨS������ (Discount) �X�⪺���~
SELECT ProductName
FROM Products
WHERE ProductID NOT IN (
  SELECT ProductID
  FROM [Order Details]
  WHERE Discount = 0
)


-- �C�X�ʶR�D���ꪺ���~���Ȥ�
SELECT DISTINCT o.CustomerID
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
JOIN Suppliers s ON p.SupplierID = s.SupplierID
WHERE s.Country <> o.ShipCountry

-- �C�X�b�P�ӫ����������q���u�i�H�A�Ȫ��Ȥ�
SELECT DISTINCT c.CustomerID, c.City
FROM Customers c
INNER JOIN Employees e ON c.City = e.City
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE e.EmployeeID = o.EmployeeID

-- �C�X���ǲ��~�S���H�R�L
SELECT ProductID, ProductName
FROM Products
WHERE NOT EXISTS (
  SELECT * FROM [Order Details] WHERE Products.ProductID = [Order Details].ProductID
)

----------------------------------------------------------------------------------------
-- �C�X�Ҧ��b�C�Ӥ�멳���q��
SELECT *
FROM Orders
WHERE DAY(EOMONTH(OrderDate)) = DAY(OrderDate)

-- �C�X�C�Ӥ�멳��X�����~
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


-- ��X���R�L�̶Q���T�Ӳ��~��������@�Ӫ��e�T�Ӥj�Ȥ�
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

-- ��X���ѹL�P����B�e�T���Ӳ��~���e�T�Ӥj�Ȥ�
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


-- ��X���R�L�P����B�e�T���Ӳ��~�������O���e�T�Ӥj�Ȥ�
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


-- �C�X���O�`���B����Ҧ��Ȥᥭ�����O�`���B���Ȥ᪺�W�r�A�H�ΫȤ᪺���O�`���B
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


-- �C�X�̼��P�����~�A�H�γQ�ʶR���`���B
SELECT TOP 1
    p.ProductName AS '���P���~�W��',
    SUM(od.Quantity) AS '�`�P��q',
    SUM(od.UnitPrice * od.Quantity) AS '�`�P����B'
FROM
    [Order Details] od
    JOIN Products p ON od.ProductID = p.ProductID
GROUP BY
    p.ProductName
ORDER BY
    SUM(od.Quantity) DESC;

-- �C�X�̤֤H�R�����~
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

-- �C�X�̨S�H�n�R�����~���O (Categories)

SELECT TOP 1 Categories.CategoryName, COUNT(*) AS TotalOrders
FROM Categories
INNER JOIN Products ON Categories.CategoryID = Products.CategoryID
INNER JOIN [Order Details] ON Products.ProductID = [Order Details].ProductID
GROUP BY Categories.CategoryName
ORDER BY TotalOrders ASC;

-- �C�X��P��̦n�������ӶR�̦h���B���Ȥ�P�ʶR���B (�t�ʶR�䥦�����Ӫ����~)
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

-- �C�X��P��̦n�������ӶR�̦h���B���Ȥ�P�ʶR���B (���t�ʶR�䥦�����Ӫ����~)
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

-- �C�X���ǲ��~�S���H�R�L

SELECT p.ProductName
FROM Products p
LEFT JOIN [Order Details] od ON p.ProductID = od.ProductID
WHERE od.OrderID IS NULL


-- �C�X�S���ǯu (Fax) ���Ȥ�M�������O�`���B
SELECT Customers.CustomerID, Customers.CompanyName, SUM(Orders.Freight) AS TotalSpent
FROM Customers
JOIN Orders ON Customers.CustomerID = Orders.CustomerID
WHERE Customers.Fax IS NULL
GROUP BY Customers.CustomerID, Customers.CompanyName

-- �C�X�C�@�ӫ������O�����~�����ƶq
SELECT Customers.City, COUNT(DISTINCT Products.CategoryID) AS NumCategories
FROM Customers
INNER JOIN Orders ON Customers.CustomerID = Orders.CustomerID
INNER JOIN [Order Details] ON Orders.OrderID = [Order Details].OrderID
INNER JOIN Products ON [Order Details].ProductID = Products.ProductID
GROUP BY Customers.City

-- �C�X�ثe�S���w�s�����~�b�L�h�`�@�Q�q�ʪ��ƶq
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

-- �C�X�ثe�S���w�s�����~�b�L�h���g�Q���ǫȤ�q�ʹL
SELECT DISTINCT c.CustomerID, o.OrderID
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
WHERE p.UnitsInStock = 0

-- �C�X�C����u���U�ݪ��~�Z�`���B
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


-- �C�X�C�a�f�B���q�B�e�̦h�����@�ز��~���O�P�`�ƶq
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

-- �C�X�C�@�ӫȤ�R�̦h�����~���O�P���B
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

-- �C�X�C�@�ӫȤ�R�̦h�����@�Ӳ��~�P�ʶR�ƶq
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

-- ���ӫ��������A��X�C�@�ӫ����̪�@���q�檺�e�f�ɶ�
SELECT ShipCity, MAX(ShippedDate) AS LatestDeliveryDate
FROM Orders
GROUP BY ShipCity

-- �C�X�ʶR���B�Ĥ��W�P�ĤQ�W���Ȥ�A�H�Ψ�ӫȤ᪺���B�t�Z
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





