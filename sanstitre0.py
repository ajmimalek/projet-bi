import psycopg2
import pygrametl
import matplotlib.pyplot as plt
import matplotlib.gridspec as grd

pgconn = psycopg2.connect(dbname='LivraisonFournisseur',user='postgres', password='0000',port='5433')
connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to "Projet"')
print('connection établie')


cursor = pgconn.cursor()

#liste pour les abscisses
listN=[]
#liste pour les valeurs
ListP=[]
#les montants entre 102999 et 104000
cursor.execute("""Select count(montant) from commande where montant >102999 and montant< 104000""")
#on recupere ses valeurs  dans une liste(listN)
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >101999 and montant< 103000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >100999 and montant< 102000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >99999 and montant< 101000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >98999 and montant< 100000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >97999 and montant< 99000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >96999 and montant< 98000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >95999 and montant< 97000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >94999 and montant< 96000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >93999 and montant< 95000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >92999 and montant< 94000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >91999 and montant< 93000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >90999 and montant< 92000""")
for row in cursor:
    ListP.append(row[0])
cursor.execute("""Select count(montant) from commande where montant >89999 and montant< 91000""")
for row in cursor:
    ListP.append(row[0])
    
ListN=[103,102,101,100,99,98,97,96,95,94,93,92,91,90]
plt.xlabel("montant en dollar")
plt.ylabel("nombre des commandes")
   
plt.plot(ListN,ListP,marker='*')
plt.title("row des commandes  supérieur à 90000")
plt.show()
ListF=[]
ListN=[]
cursor.execute("""Select sum(qte),nomfournisseur from fournisseur,commande where fournisseur.id=commande.id group by nomfournisseur having sum(qte)>2100000""")
for row in cursor:
    ListF.append(row[0])
    ListN.append(row[1])
explode=(0,0.5,0.25,0,0,0)    
the_grid = grd.GridSpec(2, 1)
plt.subplot(the_grid[0, 0], aspect=1)
plt.pie(ListF,explode=explode,labels=ListF,autopct='%1.1f%%',startangle=90,shadow=True)
plt.axis('equal')
plt.title("Les Fournisseurs ayant Une Quantité Commandé Supérieure à 2100000")
plt.subplot(the_grid[1,0], aspect=1)
plt.pie(ListF,explode=explode,labels=ListN,autopct='%1.1f%%',startangle=90,shadow=True)
plt.axis('equal')


plt.show()
ListF=[]
ListN=[]
cursor.execute("""Select max(qte) from commande """)
for row in cursor:
    ListF.append(row[0])
cursor.execute("""Select min(qte) from commande """)
for row in cursor:
    ListF.append(row[0])
cursor.execute("""Select avg(qte) from commande """)
for row in cursor:
    ListF.append(row[0])
  

    
plt.hist(ListF,bins=100) 
plt.title("La quantité minimale d'une commande , la Moyenne des Commandes , La valeur maximale d'une commande")
plt.show()  
connection.commit()
connection.close()
# Finally the connection to the sales database is closed
pgconn.close()