
--How many total messages are being sent every day?
select createdAt, count(message) as message_count from dataengineering.messages group by date_format(createdAt,'%Y-%m-%d') order by createdAt desc;;
	
--Are there any users that did not receive any message?
select u.* from dataengineering.users u left join dataengineering.messages m on u.id=m.receiverId where m.receiverId is null;

--How many active subscriptions do we have today?
select date_format(createdAt,'%Y-%m-%d') as createdAt, count(status) as active_status_count
from dataengineering.subscriptions where date_format(createdAt,'%Y-%m-%d')= date_format(now(),'%Y-%m-%d') and status='Active';

--Are there users sending messages without an active subscription? (some extra context for you: in our apps only premium users can send messages).
select users.* from dataengineering.users users join
(select distinct id from dataengineering.subscriptions where id not in
(select distinct id from dataengineering.subscriptions where status = 'Active')) subs on users.id = subs.id

--messages table creation
CREATE TABLE dataengineering.`messages` (
  `id` int DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  `receiverId` int DEFAULT NULL,
  `senderId` int DEFAULT NULL,
  `message` text
);

--subscriptions table creation
CREATE TABLE dataengineering.`subscriptions` (
  `id` int DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  `startDate` datetime DEFAULT NULL,
  `endDate` datetime DEFAULT NULL,
  `status` text,
  `amount` double DEFAULT NULL
);

--users table creation
CREATE TABLE dataengineering.`users` (
  `id` int DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  `updatedAt` datetime DEFAULT NULL,
  `firstName` text,
  `lastName` text,
  `address` text,
  `city` text,
  `country` text,
  `zipCode` text,
  `email` text,
  `birthDate` datetime DEFAULT NULL,
  `profile.gender` text,
  `profile.isSmoking` tinyint(1) DEFAULT NULL,
  `profile.profession` text,
  `profile.income` text
);

--View creation for final users (Removed PII Data)
create view dataengineering.users_view as select id, createdAt, updatedAt from dataengineering.users;

create view dataengineering.subscriptions_view as select id, createdAt, startDate, endDate, status from dataengineering.subscriptions;

create view dataengineering.messages_view as select id, createdAt, receiverId, senderId from dataengineering.messages;
