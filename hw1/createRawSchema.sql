create table Pub (k text, p text);
create table Field (k text, i text, p text, v text);
copy Pub from '/Users/Chaofan/Downloads/CSE544/hw1/pubFile.txt';
copy Field from '/Users/Chaofan/Downloads/CSE544/hw1/fieldFile.txt';