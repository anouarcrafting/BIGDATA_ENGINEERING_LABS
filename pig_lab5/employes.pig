-- Chargement des employés : id, nom, prenom, depno, ville, salaire, sexe
employees = LOAD '/input/employees.txt'
            USING PigStorage(',')
            AS (id:int, nom:chararray, prenom:chararray, depno:int, ville:chararray, salaire:int, sexe:chararray);

-- Chargement des départements : depno, nom du département
departements = LOAD '/input/departements.txt'
               USING PigStorage(',')
               AS (depno:int, dname:chararray);



-- 1. Salaire moyen des employés par département

grp_dep = GROUP employees BY depno;
avg_sal = FOREACH grp_dep GENERATE group AS depno, AVG(employees.salaire) AS salaire_moyen;



-- 2. Nombre d'employés par département

count_emp = FOREACH grp_dep GENERATE group AS depno, COUNT(employees) AS nb_employes;



-- 3. Liste des employés avec leur département

emp_dept = JOIN employees BY depno, departements BY depno;



-- 4. Employés ayant un salaire > 60000

emp_high_salary = FILTER employees BY salaire > 60000;



-- 5. Département avec le salaire le plus élevé

max_sal = FOREACH grp_dep GENERATE group AS depno, MAX(employees.salaire) AS max_salaire;
max_sal_sorted = ORDER max_sal BY max_salaire DESC;
dept_max_sal = LIMIT max_sal_sorted 1;



-- 6. Départements sans employés

emp_dept_left = JOIN departements BY depno LEFT OUTER, employees BY depno;
dept_no_emp = FILTER emp_dept_left BY employees::id IS NULL;



-- 7. Nombre total d'employés dans l’entreprise

total_emp = FOREACH (GROUP employees ALL) GENERATE COUNT(employees);



-- 8. Employés de la ville de Paris

emp_paris = FILTER employees BY ville == 'Paris';



-- 9. Salaire total par ville

grp_ville = GROUP employees BY ville;
sal_total_ville = FOREACH grp_ville GENERATE group AS ville, SUM(employees.salaire) AS total_salaire;



-- 10. Départements où travaillent des femmes

femmes = FILTER employees BY sexe == 'F';
femmes_dept = JOIN femmes BY depno, departements BY depno;



-- 11. Sauvegarde du résultat des départements avec femmes

STORE femmes_dept INTO '/pigout/employes_femmes' USING PigStorage(',');