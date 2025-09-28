-- 用户订单数据库备份文件（MySQL版本）
-- 生成时间: 2025-09-27 22:02:36

-- 表结构
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    level VARCHAR(50) NOT NULL,
    join_date DATE NOT NULL
) ENGINE=InnoDB;

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    shipping_address TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
) ENGINE=InnoDB;

CREATE TABLE order_items (
    item_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT,
    product_id INT,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    subtotal DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders (order_id)
) ENGINE=InnoDB;

-- customers 表数据
INSERT INTO customers VALUES (1, '张芳', '普通', '2023-12-12');
INSERT INTO customers VALUES (2, '李勇', '黄金', '2023-08-14');
INSERT INTO customers VALUES (3, '吴娜', '铂金', '2023-04-12');
INSERT INTO customers VALUES (4, '张洋', '铂金', '2023-02-17');
INSERT INTO customers VALUES (5, '王明', '铂金', '2023-11-22');
INSERT INTO customers VALUES (6, '杨明', '白银', '2023-02-14');
INSERT INTO customers VALUES (7, '杨强', '普通', '2023-11-09');
INSERT INTO customers VALUES (8, '李强', '铂金', '2023-11-07');
INSERT INTO customers VALUES (9, '刘勇', '铂金', '2023-04-06');
INSERT INTO customers VALUES (10, '张娜', '钻石', '2023-11-03');
INSERT INTO customers VALUES (11, '张磊', '钻石', '2023-05-01');
INSERT INTO customers VALUES (12, '李强', '黄金', '2023-07-04');
INSERT INTO customers VALUES (13, '陈秀英', '铂金', '2023-02-11');
INSERT INTO customers VALUES (14, '吴明', '黄金', '2023-11-28');
INSERT INTO customers VALUES (15, '王伟', '白银', '2023-03-26');
INSERT INTO customers VALUES (16, '黄秀英', '黄金', '2023-11-19');
INSERT INTO customers VALUES (17, '陈强', '白银', '2023-09-14');
INSERT INTO customers VALUES (18, '杨勇', '白银', '2023-09-11');
INSERT INTO customers VALUES (19, '刘秀英', '白银', '2023-08-18');
INSERT INTO customers VALUES (20, '陈伟', '黄金', '2023-03-05');
INSERT INTO customers VALUES (21, '周洋', '铂金', '2023-01-27');
INSERT INTO customers VALUES (22, '周磊', '普通', '2023-01-01');
INSERT INTO customers VALUES (23, '黄娜', '黄金', '2023-09-04');
INSERT INTO customers VALUES (24, '王强', '黄金', '2023-11-05');
INSERT INTO customers VALUES (25, '陈磊', '钻石', '2023-09-25');
INSERT INTO customers VALUES (26, '刘磊', '黄金', '2023-09-22');
INSERT INTO customers VALUES (27, '王伟', '普通', '2023-05-28');
INSERT INTO customers VALUES (28, '王秀英', '钻石', '2023-09-07');
INSERT INTO customers VALUES (29, '吴磊', '钻石', '2023-11-15');
INSERT INTO customers VALUES (30, '刘明', '铂金', '2023-02-20');

-- orders 表数据
INSERT INTO orders VALUES (1, 5, '2025-09-18', 1803.10, '已完成', '银行卡', '上海市浦东新区路718号');
INSERT INTO orders VALUES (2, 27, '2025-07-05', 15998.00, '已完成', '微信', '上海市徐汇区路867号');
INSERT INTO orders VALUES (3, 18, '2025-07-25', 5694.00, '已完成', '微信', '上海市徐汇区路666号');
INSERT INTO orders VALUES (4, 8, '2025-09-13', 2277.15, '已完成', '支付宝', '上海市浦东新区路266号');
INSERT INTO orders VALUES (5, 19, '2025-09-02', 43595.00, '已完成', '支付宝', '上海市浦东新区路827号');
INSERT INTO orders VALUES (6, 12, '2025-07-16', 4364.30, '已完成', '支付宝', '上海市徐汇区路60号');
INSERT INTO orders VALUES (7, 6, '2025-09-10', 31195.00, '已完成', '微信', '上海市浦东新区路866号');
INSERT INTO orders VALUES (8, 3, '2025-08-10', 30393.35, '已完成', '微信', '上海市静安区路846号');
INSERT INTO orders VALUES (9, 10, '2025-08-31', 22224.30, '已完成', '微信', '上海市徐汇区路583号');
INSERT INTO orders VALUES (10, 26, '2025-09-09', 25074.30, '已完成', '支付宝', '上海市徐汇区路659号');
INSERT INTO orders VALUES (11, 22, '2025-08-21', 19895.00, '已完成', '支付宝', '上海市徐汇区路793号');
INSERT INTO orders VALUES (12, 3, '2025-07-12', 14247.15, '已完成', '支付宝', '上海市浦东新区路529号');
INSERT INTO orders VALUES (13, 9, '2025-08-23', 5315.25, '已完成', '银行卡', '上海市浦东新区路798号');
INSERT INTO orders VALUES (14, 24, '2025-09-18', 9874.30, '已完成', '银行卡', '上海市浦东新区路984号');
INSERT INTO orders VALUES (15, 11, '2025-08-14', 1802.15, '已完成', '支付宝', '上海市静安区路188号');
INSERT INTO orders VALUES (16, 25, '2025-08-15', 3890.25, '已完成', '微信', '上海市徐汇区路814号');
INSERT INTO orders VALUES (17, 28, '2025-09-20', 10257.15, '已完成', '微信', '上海市徐汇区路851号');
INSERT INTO orders VALUES (18, 15, '2025-07-17', 17297.00, '已完成', '支付宝', '上海市徐汇区路644号');
INSERT INTO orders VALUES (19, 1, '2025-09-24', 4496.00, '配送中', '支付宝', '上海市静安区路558号');
INSERT INTO orders VALUES (20, 22, '2025-06-29', 51294.00, '已完成', '微信', '上海市徐汇区路566号');
INSERT INTO orders VALUES (21, 15, '2025-08-17', 598.00, '已完成', '银行卡', '上海市静安区路519号');
INSERT INTO orders VALUES (22, 9, '2025-08-19', 3797.15, '已完成', '微信', '上海市浦东新区路181号');
INSERT INTO orders VALUES (23, 3, '2025-09-15', 32293.35, '已完成', '银行卡', '上海市浦东新区路784号');
INSERT INTO orders VALUES (24, 18, '2025-07-31', 897.00, '已完成', '微信', '上海市浦东新区路203号');
INSERT INTO orders VALUES (25, 21, '2025-08-05', 12060.25, '已完成', '微信', '上海市静安区路906号');
INSERT INTO orders VALUES (26, 5, '2025-09-10', 1518.10, '已完成', '微信', '上海市徐汇区路334号');
INSERT INTO orders VALUES (27, 27, '2025-09-25', 16895.00, '配送中', '支付宝', '上海市浦东新区路272号');
INSERT INTO orders VALUES (28, 16, '2025-09-13', 7122.15, '已完成', '支付宝', '上海市静安区路785号');
INSERT INTO orders VALUES (29, 2, '2025-09-02', 15766.20, '已完成', '微信', '上海市浦东新区路740号');
INSERT INTO orders VALUES (30, 3, '2025-08-13', 15198.10, '已完成', '微信', '上海市徐汇区路636号');

-- order_items 表数据
INSERT INTO order_items VALUES (1, 1, 8, '书包', 1, 284.05, 284.05);
INSERT INTO order_items VALUES (2, 1, 3, '咖啡机', 1, 1519.05, 1519.05);
INSERT INTO order_items VALUES (3, 2, 1, 'iPhone 15 Pro', 2, 7999.00, 15998.00);
INSERT INTO order_items VALUES (4, 3, 4, '蓝牙耳机', 1, 799.00, 799.00);
INSERT INTO order_items VALUES (5, 3, 6, '办公椅', 3, 1299.00, 3897.00);
INSERT INTO order_items VALUES (6, 3, 5, '运动鞋', 2, 499.00, 998.00);
INSERT INTO order_items VALUES (7, 4, 4, '蓝牙耳机', 3, 759.05, 2277.15);
INSERT INTO order_items VALUES (8, 5, 10, '平板电脑', 1, 3599.00, 3599.00);
INSERT INTO order_items VALUES (9, 5, 2, 'MacBook Air', 1, 9999.00, 9999.00);
INSERT INTO order_items VALUES (10, 5, 2, 'MacBook Air', 3, 9999.00, 29997.00);
INSERT INTO order_items VALUES (11, 6, 5, '运动鞋', 1, 474.05, 474.05);
INSERT INTO order_items VALUES (12, 6, 9, '电动牙刷', 3, 284.05, 852.15);
INSERT INTO order_items VALUES (13, 6, 3, '咖啡机', 2, 1519.05, 3038.10);
INSERT INTO order_items VALUES (14, 7, 10, '平板电脑', 2, 3599.00, 7198.00);
INSERT INTO order_items VALUES (15, 7, 1, 'iPhone 15 Pro', 3, 7999.00, 23997.00);
INSERT INTO order_items VALUES (16, 8, 5, '运动鞋', 1, 474.05, 474.05);
INSERT INTO order_items VALUES (17, 8, 1, 'iPhone 15 Pro', 3, 7599.05, 22797.15);
INSERT INTO order_items VALUES (18, 8, 7, '智能手表', 3, 2374.05, 7122.15);
INSERT INTO order_items VALUES (19, 9, 9, '电动牙刷', 3, 284.05, 852.15);
INSERT INTO order_items VALUES (20, 9, 7, '智能手表', 1, 2374.05, 2374.05);
INSERT INTO order_items VALUES (21, 9, 2, 'MacBook Air', 2, 9499.05, 18998.10);
INSERT INTO order_items VALUES (22, 10, 1, 'iPhone 15 Pro', 3, 7599.05, 22797.15);
INSERT INTO order_items VALUES (23, 10, 4, '蓝牙耳机', 3, 759.05, 2277.15);
INSERT INTO order_items VALUES (24, 11, 6, '办公椅', 3, 1299.00, 3897.00);
INSERT INTO order_items VALUES (25, 11, 1, 'iPhone 15 Pro', 2, 7999.00, 15998.00);
INSERT INTO order_items VALUES (26, 12, 7, '智能手表', 2, 2374.05, 4748.10);
INSERT INTO order_items VALUES (27, 12, 2, 'MacBook Air', 1, 9499.05, 9499.05);
INSERT INTO order_items VALUES (28, 13, 5, '运动鞋', 1, 474.05, 474.05);
INSERT INTO order_items VALUES (29, 13, 8, '书包', 1, 284.05, 284.05);
INSERT INTO order_items VALUES (30, 13, 3, '咖啡机', 3, 1519.05, 4557.15);
INSERT INTO order_items VALUES (31, 14, 7, '智能手表', 3, 2374.05, 7122.15);
INSERT INTO order_items VALUES (32, 14, 4, '蓝牙耳机', 2, 759.05, 1518.10);
INSERT INTO order_items VALUES (33, 14, 6, '办公椅', 1, 1234.05, 1234.05);
INSERT INTO order_items VALUES (34, 15, 8, '书包', 1, 284.05, 284.05);
INSERT INTO order_items VALUES (35, 15, 4, '蓝牙耳机', 2, 759.05, 1518.10);
INSERT INTO order_items VALUES (36, 16, 3, '咖啡机', 2, 1519.05, 3038.10);
INSERT INTO order_items VALUES (37, 16, 9, '电动牙刷', 3, 284.05, 852.15);
INSERT INTO order_items VALUES (38, 17, 10, '平板电脑', 3, 3419.05, 10257.15);
INSERT INTO order_items VALUES (39, 18, 1, 'iPhone 15 Pro', 2, 7999.00, 15998.00);
INSERT INTO order_items VALUES (40, 18, 6, '办公椅', 1, 1299.00, 1299.00);
INSERT INTO order_items VALUES (41, 19, 10, '平板电脑', 1, 3599.00, 3599.00);
INSERT INTO order_items VALUES (42, 19, 9, '电动牙刷', 3, 299.00, 897.00);
INSERT INTO order_items VALUES (43, 20, 6, '办公椅', 1, 1299.00, 1299.00);
INSERT INTO order_items VALUES (44, 20, 2, 'MacBook Air', 2, 9999.00, 19998.00);
INSERT INTO order_items VALUES (45, 20, 2, 'MacBook Air', 3, 9999.00, 29997.00);
INSERT INTO order_items VALUES (46, 21, 8, '书包', 2, 299.00, 598.00);
INSERT INTO order_items VALUES (47, 22, 3, '咖啡机', 1, 1519.05, 1519.05);
INSERT INTO order_items VALUES (48, 22, 3, '咖啡机', 1, 1519.05, 1519.05);
INSERT INTO order_items VALUES (49, 22, 4, '蓝牙耳机', 1, 759.05, 759.05);
INSERT INTO order_items VALUES (50, 23, 3, '咖啡机', 1, 1519.05, 1519.05);
INSERT INTO order_items VALUES (51, 23, 4, '蓝牙耳机', 3, 759.05, 2277.15);
INSERT INTO order_items VALUES (52, 23, 2, 'MacBook Air', 3, 9499.05, 28497.15);
INSERT INTO order_items VALUES (53, 24, 9, '电动牙刷', 2, 299.00, 598.00);
INSERT INTO order_items VALUES (54, 24, 9, '电动牙刷', 1, 299.00, 299.00);
INSERT INTO order_items VALUES (55, 25, 2, 'MacBook Air', 1, 9499.05, 9499.05);
INSERT INTO order_items VALUES (56, 25, 4, '蓝牙耳机', 3, 759.05, 2277.15);
INSERT INTO order_items VALUES (57, 25, 9, '电动牙刷', 1, 284.05, 284.05);
INSERT INTO order_items VALUES (58, 26, 4, '蓝牙耳机', 2, 759.05, 1518.10);
INSERT INTO order_items VALUES (59, 27, 8, '书包', 1, 299.00, 299.00);
INSERT INTO order_items VALUES (60, 27, 1, 'iPhone 15 Pro', 2, 7999.00, 15998.00);
INSERT INTO order_items VALUES (61, 27, 9, '电动牙刷', 2, 299.00, 598.00);
INSERT INTO order_items VALUES (62, 28, 7, '智能手表', 3, 2374.05, 7122.15);
INSERT INTO order_items VALUES (63, 29, 1, 'iPhone 15 Pro', 2, 7599.05, 15198.10);
INSERT INTO order_items VALUES (64, 29, 8, '书包', 2, 284.05, 568.10);
INSERT INTO order_items VALUES (65, 30, 1, 'iPhone 15 Pro', 2, 7599.05, 15198.10);