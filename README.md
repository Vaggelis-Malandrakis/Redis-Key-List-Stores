## Project #2 – Redis / Key-Value Stores
*Due Date: May 5th, 2019*

### Installation

**MySQL**

- Συνδεόμαστε με τη βάση δίνοντας: *sudo mysql -u root -p*
- Δημιουργείστε τη βάση redis_db εισάγοντας τις ακόλουθες εντολές SQL:

```
mysql> CREATE DATABASE redis_db CHARACTER SET utf8 COLLATE utf8_general_ci;
mysql> CREATE USER 'redis_user'@'localhost' IDENTIFIED BY 'password';   
mysql> GRANT ALL PRIVILEGES ON redis_db.* TO 'redis_user'@'localhost';    
```

- Δημιουργούμε πίνακα στην βάση:

```
CREATE TABLE `redis_db`.`Sales` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `cust_id` VARCHAR(45) NULL,
  `trans_id` INT NULL,
  PRIMARY KEY (`id`));
  
CREATE TABLE `redis_db`.`Transactions` (
  `trans_id` INT NOT NULL AUTO_INCREMENT,
  `value` REAL NULL,
  PRIMARY KEY (`trans_id`));
```

- Εισάγουμε τα δεδομένα στην βάση με την εκτέλεση της ακόλουθης εντολής:

```
INSERT INTO `redis_db`.`Sales` VALUES ('0', 'store1:c1', '1'), ('1', 'store1:c1', '5'), 
('2', 'store1:c1', '20'), ('3', 'store1:c2', '1'), ('4', 'store1:c2', '2'),
('5', 'store2:c2', '9'), ('6', 'store3:c2', '10'), ('7', 'store3:c2', '11'),
('8', 'store1:c3', '2'), ('9', 'store1:c3', '4'), ('10', 'store3:c3', '5'), 
('11', 'store3:c3', '9'), '12', 'store1:c4', '4'), ('13', 'store2:c4', '6'), 
('14', 'store2:c4', '10'), ('15', 'store2:c4', '12');

INSERT INTO `redis_db`.`Transactions` VALUES ('0', '1', '50'), ('1', '1', '80'),
('2', '1', '90'), ('3', '2', '65'), ('4', '3', '66'), ('5', '3', '67'), '6', '3', '68'),
('7', '3', '69'), ('8', '4', '70'), ('9', '4', '71'), ('10', '5', '72'), ('11', '6', '73'),
('12', '7', '74'), ('13', '7', '75')('14', '8', '76'), ('15', '8', '77');
```

### Functions
*Details on each function can be found in the Python files*
- **Create_KLStore** *(name, data-source, query-string, position1, position2, direction)*
- **Filter_KLStore** *(name1, expression)*
- **Apply_KLStore** *(name1, func)*
- **Aggr_KLStore** *(name1, aggr)*
- **LookUp_KLStore** *(name1, name2)*
- **ProjSel_KLStore** *(output_name, pname1, pname2, …, pnamek, expression)*

### Description
A key-value store is a system that manages a collection of *(key,value)* pairs, where key is unique in this universe. Redis – and other systems – allow the value to be a single value (e.g. string, number), a set of values, a list of values, a hash, etc.

Assume a collection of (key, list) pairs, i.e. the value is a list of values, namely strings. Assume that key is a string as well. Let’s call such a collection a *Key-List Store (KL Store)*. This is a special case of a multi-map data structure, where several values are mapped to a key.

**Example:**

| Key 	| List                   	|
|-----	|------------------------	|
| 12  	| [t12, t67]             	|
| 34  	| [t87, t12, t98]        	|
| ... 	| ...                    	|
| 76  	| [t121, t72, t99, t179] 	|

Assume two domains of values **D1** and **D2**  
e.g. D1 = {all possible customer ids}, D2 = {all possible transaction ids}

Assume that there is a process P that generates a collection of value1, value2 pairs:

**S = { (u,v): u ∈ D1 , v ∈ D2}**

Examples of such processes:
- SELECT custID, transID FROM SALES
- Reading a CSV file and getting for each line forming a pair using columns i and j.
- Running any program that produces a stream of pairs of values

Given a collection S described as above, one can define two KLStores, **KL1(S)** and **KL2(S)** as follows:

- KL1(S) = {(x, Lx), ∀x ∈ U = {u: (u,v) ∈ S}, Lx = the list of values v, such that (x,v) ∈ S}

- KL2(S) = {(x, Lx), ∀x ∈ V = {v: (u,v) ∈ S}, Lx = the list of values u, such that (u,x) ∈ S}

We want to implement in Python the following functions/methods that get one or more KL stores and “return” (or update) a KL store. All these KL stores should exist in Redis.
