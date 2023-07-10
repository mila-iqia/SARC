"""
This produces a list of all the students and their supervisors.

Some students have multiple supervisors, and some supervisors are not affiliated with Mila.
We refer to people with their mila email address, but we have to make exceptions
when profs are not affiliated with Mila.
"""

import csv
from names_matching import find_exact_bag_of_words_matches
import json
import re
from datetime import datetime




# hugo.Larochelle avec une majuscule?
# guillaume.dumas@mila.quebec avec aucun étudiant? je ne vois pas de g.dumas
# Mirko Ravanelli étant un étudiant de yoshua même s'il est membre académique?
# on a un étudiant de Will Hamilton mais lui-même n'est pas là
# rioult et g.rioult, mais pas de prof avec un nom de famille "rioult"
# a.tapp sans que Alain Tapp ne soit un prof
# On dirait que Brice Rauby est supervisé par Maxime Gasse, mais Maxime Gasse était un postdoc avec Andrea Lodi. Il n'est pas prof associé à Mila, mais Brice Rauby est encore son étudiant et il a un compte actif chez nous?
# w.hamilton 1
# g.rioult 1
# rioult 1
# Deux fois siamak
#    "siamak_" : "siamak.ravanbakhsh@mila.quebec" # ??
#    "s.ravanbakhsh" : "siamak.ravanbakhsh@mila.quebec",
# Christopher Pal fait partie de sa propre liste "c.pal-students"
# Quentin BERTRAND a un nom de famille en majuscules.
# JONAS NGNAWE
# DOHA, HWANG
# Yue Li is on her own mailing list y.li-students
# PerreaultLevasseur avec un nom de famille sans trait d'union
# AbbasgholizadehRahimi avec un nom de famille sans trait d'union
# qqch de bizarre avec Sarath Chandar dont "Sarath Chandar" semble être seulement le prénom,
#     mais le site du Mila suggère quasiment que c'est son prénom et son nom de famille
# EbrahimiKahou devrait être Ebrahimi-Kahou

mapping_group_to_prof = {
    "y.bengio" : "yoshua.bengio@mila.quebec",
    "c.gagne" : "christian.gagne@mila.quebec",
    "s.ebrahimi.kahou" : "ebrahims@mila.quebec",
    "i.rish" : "irina.rish@mila.quebec",
    "l.paull" : "paulll@mila.quebec",
    "l.perreault-levasseur" : "levassel@mila.quebec",
    "m.ravanelli" : "ravanelm@mila.quebec",
    "g.berseth" : "glen.berseth@mila.quebec",
    "a.courville" : "courvila@mila.quebec",
    "m.bellemare" : "bellemam@mila.quebec",
    "g.farnadi": "farnadig@mila.quebec",
    "d.rolnick" : "drolnick@mila.quebec",
    "g.wolf" : "wolfguy@mila.quebec",
    "a.mahajan" : "mahajana@mila.quebec",
    "f.khomh" : "foutse.khomh@mila.quebec",
    "n.leroux" : "lerouxni@mila.quebec",
    "j.cheung" : "cheungja@mila.quebec",
    "x.liu" : "liuxue@mila.quebec",
    "b.liu" : "bang.liu@mila.quebec",
    "b.richards" : "blake.richards@mila.quebec",
    "g.gidel" : "gidelgau@mila.quebec",
    "j.guo" : "guojin@mila.quebec",
    "g.lajoie" : "guillaume.lajoie@mila.quebec",
    "d.nowrouzezahrai": "derek@mila.quebec",
    "g.rabusseau": "rabussgu@mila.quebec",
    "s.chandar" : "sarath.chandar@mila.quebec",
    "n.armanfard" : "narges.armanfard@mila.quebec",
    "a.emad" : "emadamin@mila.quebec",
    "l.charlin" : "lcharlin@mila.quebec",
    "d.precup" : "precupdo@mila.quebec",
    "t.odonnell" : "odonnelt@mila.quebec",
    "s.reddy": "siva.reddy@mila.quebec",
    "s.rahimi": "samira.rahimi@mila.quebec",
    "j.cohen-adad" : "julien.cohen-adad@mila.quebec",
    "f.diaz" : "fernando.diaz@mila.quebec",
    "pl.bacon" : "pierre-luc.bacon@mila.quebec",
    "p.bashivan" : "bashivap@mila.quebec",
    "k.jerbi" : "karim.jerbi@mila.quebec",
    "r.rabbany" : "reihaneh.rabbany@mila.quebec",
    "s.lacoste": "slacoste@mila.quebec",
    "d.bzdok" : "bzdokdan@mila.quebec",
    "t.arbel" : "arbeltal@mila.quebec",
    "j.tang" : "tangjian@mila.quebec",
    "d.bahdanau": "bahdanau@mila.quebec",
    "s.ravanbakhsh" : "siamak.ravanbakhsh@mila.quebec",
    "c.pal" : "christopher.pal@mila.quebec",
    "j.pineau" : "jpineau@mila.quebec",
    "p.vincent" : "vincentp@mila.quebec",
    "p.panangaden" : "prakash.panangaden@mila.quebec",
    "h.larochelle": "hugo.Larochelle@mila.quebec",
    "m.gasse" : "gassemax@mila.quebec",
    "e.muller" : "eilif.muller@mila.quebec",
    "g.dudek" : "gregory.dudek@mila.quebec",
    "m.a.dilhac" : "dilhacma@mila.quebec",
    "x.si" : "xujie.si@mila.quebec",
    "d.buckeridge" : "david.buckeridge@mila.quebec",
    "i.mitliagkas" : "ioannis@mila.quebec",
    "y.li" : "liyue@mila.quebec",
    "a.romero-soriano" : "adriana.romero-soriano@mila.quebec",
    "m.blanchette" : "mathieu.blanchette@mila.quebec",
    "s.enger" : "shirin.enger@mila.quebec",
    "a.lodi": "lodiandr@mila.quebec",
    "a.moon" : "moonajung@mila.quebec",
    "a.oberman" : "adam.oberman@mila.quebec",
    "d.hjelm" : "hjelmdev@mila.quebec",
    "a.agrawal" : "aishwarya.agrawal@mila.quebec",
    "c.dubach" : "christophe.dubach@mila.quebec",
    "c.regis" : "catherine.regis@mila.quebec",
    "a.durand" : "duranda@mila.quebec",
    "d.meger" : "david.meger@mila.quebec",
    "a.huang" : "anna.huang@mila.quebec",
    "d.beaini" : "dominique.beaini@mila.quebec",
    "e.frejinger" : "frejinge@mila.quebec",
    "d.sridhar" : "dhanya.sridhar@mila.quebec",
    "k.siddiqi" : "kaleem.siddiqi@mila.quebec",
    "e.belilovsky": "belilove@mila.quebec",
    # profs no longer at Mila
    "a.tapp" : None,
    "w.hamilton" : None,
    "g.rioult": None,
    "rioult": None
}

S_profs = {v for v in mapping_group_to_prof.values() if v is not None}


mapping_prof_mila_email_to_academic_email = {
  "adam.oberman@mila.quebec": "adam.oberman@mcgill.ca",
  "adriana.romero-soriano@mila.quebec": None,
  "aishwarya.agrawal@mila.quebec": "aishwarya.agrawal@mila.quebec",
  "anna.huang@mila.quebec": None,
  "arbeltal@mila.quebec": "tal.arbel@mcgill.ca",
  "bang.liu@mila.quebec": "bang.liu@umontreal.ca",
  "bahdanau@mila.quebec": None,
  "bashivap@mila.quebec": "pouya.bashivan@mcgill.ca",
  "belilove@mila.quebec": "eugene.belilovsky@umontreal.ca",
  "bellemam@mila.quebec": None,
  "blake.richards@mila.quebec": "blake.richards@mcgill.ca",
  "bzdokdan@mila.quebec": "danilo.bzdok@mcgill.ca",
  "catherine.regis@mila.quebec": "catherine.regis@umontreal.ca",
  "cheungja@mila.quebec": "jcheung@cs.mcgill.ca",
  "christian.gagne@mila.quebec": "christian.gagne@gel.ulaval.ca",
  "christophe.dubach@mila.quebec": "christophe.dubach@mcgill.ca",
  "christopher.pal@mila.quebec": "christopher.pal@polymtl.ca",
  "courvila@mila.quebec": "aaron.courville@umontreal.ca",
  "david.buckeridge@mila.quebec": "david.buckeridge@mcgill.ca",
  "david.meger@mila.quebec": "dmeger@cim.mcgill.ca",
  "derek@mila.quebec": "derek@cim.mcgill.ca",
  "dhanya.sridhar@mila.quebec": "dhanya.sridhar@umontreal.ca",
  "dilhacma@mila.quebec": "marc-antoine.dilhac@umontreal.ca",
  "dominique.beaini@mila.quebec": "dominique.beaini@umontreal.ca",
  "drolnick@mila.quebec": "drolnick@cs.mcgill.ca",
  "duranda@mila.quebec": "audrey.durand@ift.ulaval.ca",
  "ebrahims@mila.quebec": "samira.ebrahimi-kahou@etsmtl.ca",
  "eilif.muller@mila.quebec": "eilif.muller@umontreal.ca",
  "emadamin@mila.quebec": "amin.emad@mcgill.ca",
  "farnadig@mila.quebec": "golnoosh.farnadi@hec.ca",
  "fernando.diaz@mila.quebec": None,
  "frejinge@mila.quebec": "frejinger.umontreal@gmail.com",
  "foutse.khomh@mila.quebec": "foutse.khomh@polymtl.ca",
  "glen.berseth@mila.quebec": "glen.berseth@umontreal.ca",
  "gidelgau@mila.quebec": "gauthier.gidel@umontreal.ca",
  "gregory.dudek@mila.quebec": "dudek@cs.mcgill.ca",
  "guillaume.lajoie@mila.quebec": "g.lajoie@umontreal.ca",
  "guojin@mila.quebec": "jin.guo@mcgill.ca",
  "hjelmdev@mila.quebec": "rex.devon.hjelm@umontreal.ca",
  "hugo.larochelle@mila.quebec": "hugo.larochelle@umontreal.ca",
  "ioannis@mila.quebec": "ioannis.mitliagkas@umontreal.ca",
  "irina.rish@mila.quebec": "irina.rish@umontreal.ca",
  "jpineau@mila.quebec": "jpineau@cs.mcgill.ca",
  "julien.cohen-adad@mila.quebec": "julien.cohen-adad@polymtl.ca",
  "kaleem.siddiqi@mila.quebec": "siddiqi@cim.mcgill.ca",
  "karim.jerbi@mila.quebec": "karim.jerbi@umontreal.ca",
  "lcharlin@mila.quebec": "laurent.charlin@hec.ca",
  "levassel@mila.quebec": "laurence.perreault.levasseur@umontreal.ca",
  "lerouxni@mila.quebec": None,
  "liuxue@mila.quebec": "xueliu@cs.mcgill.ca",
  "liyue@mila.quebec": "yueli@cs.mcgill.ca",
  "lodiandr@mila.quebec": "andrea.lodi@umontreal.ca",
  "mahajana@mila.quebec": "aditya.mahajan@mcgill.ca",
  "mathieu.blanchette@mila.quebec": "blanchem@cs.mcgill.ca",
  "moonajung@mila.quebec": "ajung.moon@mcgill.ca",
  "narges.armanfard@mila.quebec": "narges.armanfard@mcgill.ca",
  "odonnelt@mila.quebec": "timothy.odonnell@mcgill.ca",
  "paulll@mila.quebec": "paulll@iro.umontreal.ca",
  "pierre-luc.bacon@mila.quebec": "pierre-luc.bacon@umontreal.ca",
  "prakash.panangaden@mila.quebec": "prakash@cs.mcgill.ca",
  "precupdo@mila.quebec": "dprecup@cs.mcgill.ca",
  "rabussgu@mila.quebec": "guillaume.rabusseau@umontreal.ca",
  "ravanelm@mila.quebec": "mirco.ravanelli@concordia.ca",
  "reihaneh.rabbany@mila.quebec": "rrabba@cs.mcgill.ca",
  "samira.rahimi@mila.quebec": "samira.rahimi@mcgill.ca",
  "sarath.chandar@mila.quebec": "sarath-chandar.anbil-parthipan@polymtl.ca",
  "shirin.enger@mila.quebec": "shirin.enger@mcgill.ca",
  "siamak.ravanbakhsh@mila.quebec": "siamak@cs.mcgill.ca",
  "slacoste@mila.quebec": "slacoste@iro.umontreal.ca",
  "siva.reddy@mila.quebec": "siva.reddy@mila.quebec",
  "tangjian@mila.quebec": "jian.tang@umontreal.ca",
  "vincentp@mila.quebec": "vincentp@iro.umontreal.ca",
  "wolfguy@mila.quebec": "guy.wolf@umontreal.ca",
  "xujie.si@mila.quebec": "xsi@cs.mcgill.ca",
  "yoshua.bengio@mila.quebec": "yoshua.bengio@umontreal.ca",
}

mapping_academic_email_to_drac_info = {}
big_csv_str = """
    adam.oberman@mcgill.ca,gmu-382-01,def-oberman
    aishwarya.agrawal@mila.quebec,vrr-364-01,def-agrawal
    tal.arbel@mcgill.ca,pgf-735-01,def-arbeltal
    bang.liu@umontreal.ca,rmf-384-02,def-bangliu
    pouya.bashivan@mcgill.ca,qxe-560-02,def-bashivan
    eugene.belilovsky@umontreal.ca,cab-641-02,def-eugenium
    blake.richards@mcgill.ca,pxp-350-02,def-tyrell
    danilo.bzdok@mcgill.ca,eea-520-01,def-danilobz
    catherine.regis@umontreal.ca,None,None
    jcheung@cs.mcgill.ca,fgv-541-01,def-jcheung
    christian.gagne@gel.ulaval.ca,suj-571-01,def-chgag196
    christophe.dubach@mcgill.ca,xjt-741-01,def-cdubach
    christopher.pal@polymtl.ca,mmt-425-01,def-pal
    aaron.courville@umontreal.ca,dnb-265-02,def-courvill
    david.buckeridge@mcgill.ca,fxu-971-01,def-dbuckeri
    dmeger@cim.mcgill.ca,ipq-582-03,def-dpmeger
    derek@cim.mcgill.ca,hbm-700-02,def-dnowrouz
    dhanya.sridhar@umontreal.ca,xwa-094-01,def-dsridhar
    marc-antoine.dilhac@umontreal.ca,None,None
    dominique.beaini@umontreal.ca,hwe-254-01,def-pr61079
    drolnick@cs.mcgill.ca,bvv-703-01,def-drolnick
    audrey.durand@ift.ulaval.ca,sks-000-03,def-adurand
    samira.ebrahimi-kahou@etsmtl.ca,bzg-655-05,def-ebrahimi
    eilif.muller@umontreal.ca,wgc-040-01,def-emuller
    amin.emad@mcgill.ca,tjs-131-01,def-aminemad
    golnoosh.farnadi@hec.ca,cbh-860-04,def-gfarnadi
    frejinger.umontreal@gmail.com,axr-482-01,def-frejinge
    foutse.khomh@polymtl.ca,vva-480-01,def-foutsekh
    glen.berseth@umontreal.ca,fju-421-02,def-gberseth
    gauthier.gidel@umontreal.ca,vsd-820-02,def-gidelgau
    dudek@cs.mcgill.ca,aya-314-01(deactivated),def-dudek
    g.lajoie@umontreal.ca,uwq-771-01,def-glaj
    jin.guo@mcgill.ca,pxs-500-01,def-jinguo
    rex.devon.hjelm@umontreal.ca,None,None
    hugo.larochelle@umontreal.ca,ycy-622-03(deactivated),def-laroche1
    ioannis.mitliagkas@umontreal.ca,kad-164-01,def-ioannism
    irina.rish@umontreal.ca,mnj-282-01,def-irina
    jpineau@cs.mcgill.ca,jim-594-01,def-jpineau
    julien.cohen-adad@polymtl.ca,rrp-355-01,def-jcohen
    siddiqi@cim.mcgill.ca,jqt-923-01,def-siddiqi
    karim.jerbi@umontreal.ca,kif-392-01,def-kjerbi
    laurent.charlin@hec.ca,kfr-353-03,def-lcharlin
    laurence.perreault.levasseur@umontreal.ca,jyf-835-01,def-lplevass
    xueliu@cs.mcgill.ca,tuc-100-01,def-cpsmcgil
    yueli@cs.mcgill.ca,yfh-205-01,def-liyue
    andrea.lodi@umontreal.ca,cnn-781-01,def-alodi
    aditya.mahajan@mcgill.ca,ffv-054-01,def-adityam
    blanchem@cs.mcgill.ca,yxw-673-01,def-mblanche
    ajung.moon@mcgill.ca,inq-723-01,def-amoon
    narges.armanfard@mcgill.ca,szi-293-01,def-armanfn
    timothy.odonnell@mcgill.ca,gmg-385-01,def-timod
    paulll@iro.umontreal.ca,rjx-155-01,def-lpaull
    pierre-luc.bacon@umontreal.ca,fsp-674-01,def-plbacon
    prakash@cs.mcgill.ca,byv-354-01,def-prakash9
    dprecup@cs.mcgill.ca,xzv-031-01,def-dprecup
    guillaume.rabusseau@umontreal.ca,bzd-345-03,def-grabus
    mirco.ravanelli@concordia.ca,xzx-842-03,def-ravanelm
    rrabba@cs.mcgill.ca,gtk-970-01,def-rrabba
    samira.rahimi@mcgill.ca,jjh-932-01,def-srad
    sarath-chandar.anbil-parthipan@polymtl.ca,bbj-240-04,apsarath
    shirin.enger@mcgill.ca,bgc-914-02,def-senger
    siamak@cs.mcgill.ca,vjz-631-02,def-siamakx
    slacoste@iro.umontreal.ca,bhn-306-01,def-lacosts
    siva.reddy@mila.quebec,hsf-443-01,def-sreddy
    jian.tang@umontreal.ca,xmk-590-01,def-tjhec
    vincentp@iro.umontreal.ca,jme-614-01,def-pascal
    guy.wolf@umontreal.ca,cii-306-01,def-wolfg
    xsi@cs.mcgill.ca,nza-075-01,def-six
    yoshua.bengio@umontreal.ca,jvb-000-01,def-bengioy
"""
for e in big_csv_str.split("\n"):
    if len(e) > 10:
        academic_email, ccri, def_account = e.split(",")
        academic_email = academic_email.strip()
        if "deactivated" in ccri.lower() or "none" in ccri.lower():
            ccri = None
        if "none" in def_account.lower():
            def_account = None
        mapping_academic_email_to_drac_info[academic_email] = (ccri, def_account)
        

"""
"cn=core-academic-member,ou=Groups,dc=mila,dc=quebec",
            "cn=downscaling,ou=Groups,dc=mila,dc=quebec",
            "cn=drolnick-ml4climate,ou=Groups,dc=mila,dc=quebec",
            "cn=ecosystem-embeddings,ou=Groups,dc=mila,dc=quebec",
            "cn=internal,ou=Groups,dc=mila,dc=quebec",
            "cn=mila-core-profs,ou=Groups,dc=mila,dc=quebec",
            "cn=mila-profs,ou=Groups,dc=mila,dc=quebec",
"""

S_profs_cn_groups = {"mila-core-profs", "mila-profs", "core-academic-member",
                     "core-industry-member", "associate-member-academic",
                     "external-associate-members",
                     "associate-member-industry"}

# "aishwarya_lab" is basically the same as "a.agrawal" but it also contains
# "Yash" who works at Samsung downstairs.
# Since he's not a student, we don't want to have the "aishwarya_lab" mention
# in the students.
# There's 'ioannis.collab' and 'gidel.lab that I'm not sure about.
# "linclab_users" et "linclab" ?
S_cn_groups_to_ignore = {
    "deactivation-2022", "fall2022", "covid19-app-dev", "internal",
    "townhall", "aishwarya_lab", "edi.survey.students", "computer-vision-rg",
    "2fa-reminder", "overleaf_renewal", "gflownet", "neural-ai-rg", "winter2022-associate",
    "downscaling", "overleaf_renewal", "onboarding-cluster", "ecosystem-embeddings",
    "ccai", "covid19-helpers", "thisclimate", "vicc", "aiphysim_users"}
# statcan.dialogue.dataset, te_dk_xm (not sure what this is), 

class Prof:
    def __init__(self, first_name, last_name, mila_email_username, cn_groups:set):
        # when we have a Mila prof, there needs to be some way of
        # placing them in some of the cn_groups that makes them profs
        if mila_email_username is not None:
            assert set(cn_groups).intersection(S_profs_cn_groups), (
                f"Prof {first_name} {last_name} ({mila_email_username}) doesn't belong to the right groups."
            )

        self.first_name = first_name.replace(' ', '')
        self.last_name = last_name.replace(' ', '')
        self.mila_email_username = mila_email_username.lower()
        self.academic_email = mapping_prof_mila_email_to_academic_email[self.mila_email_username]
        self.cn_groups = cn_groups.difference(S_cn_groups_to_ignore)
        # let's give an opportunity to override with the constructor
        if self.academic_email in mapping_academic_email_to_drac_info:
            self.ccri, self.def_account = mapping_academic_email_to_drac_info[self.academic_email]
        else:
            self.ccri, self.def_account = None, None

class Student:
    def __init__(self, first_name, last_name, mila_email_username, cn_groups:set,
                 supervisor=None, co_supervisor=None, university=None):
        self.first_name = first_name
        self.last_name = last_name
        self.mila_email_username = mila_email_username
        self.cn_groups = cn_groups.difference(S_cn_groups_to_ignore)
        self.supervisor = supervisor
        self.co_supervisor = co_supervisor
        self.university = university


def read_population_mila_csv(input_path):
    """
    Process each line with a dictionary reader.
    Resolve the conflits when there is more than
    one entry for a given person because they were
    at Mila at different times.
    """

    today = datetime.now()
    formatted_today = today.strftime("%Y-%m-%d")

    L_excel_students = []
    with open(input_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        # LD_members = [line for line in csv_reader]
        for person in csv_reader:
            
            if person['EndDate'] <= formatted_today:
                continue
            elif person['StartDate'] >= formatted_today:
                continue

            L_excel_students.append(
                {"first_name": person["FirstName"],
                   "last_name": person["LastName"],
                   "supervisor": person["Supervisor"],
                   "cosupervisor": person["CoSupervisor"]})
    return L_excel_students

def read_mila_raw_ldap_json(input_path):
    """
    Read the JSON file.
    """

    #def process(e):
    #    if re.match()

    L_profs = []
    L_students = []

    with open(input_path, 'r') as json_file:
        json_data = json.load(json_file)

        for person in json_data:

            # the most straightforward way to determine if a person is a prof,
            # because you can't trust the cn_groups "core-profs" where 
            # the mila directors are also listed
            is_prof = person["mail"][0] in S_profs

            is_student = False
            cn_groups_of_supervisors = []
            cn_groups = []
            university = None

            if person["suspended"][0] == 'true':
                continue

            for e in person['memberOf']:
                if m := re.match(r"^cn=(.+?)-students.*", e):
                    if m.group(1) in ["mcgill", "udem", "poly", "ets", "concordia", "ulaval", "hec"]:
                        university = m.group(1)
                        continue
                    cn_groups_of_supervisors.append(m.group(1))
                    is_student = True
                    continue
                if m := re.match(r"^cn=(.+?),.*", e):
                    # if m.group(1) in ["mila-core-profs", "mila-profs", "core-academic-member"]:
                    #     is_prof = True
                    cn_groups.append(m.group(1))
                    continue

            if person['mail'][0] in ["christopher.pal@mila.quebec", "liyue@mila.quebec", "ravanelm@mila.quebec"]:
                # For some reason, Christopher Pal and Yue Li are on their own students lists.
                # Mirco Ravanelli is an ex postdoc of Yoshua but appears to be an associate member now. 
                # Let's make exceptions.
                is_student = False
            elif person['mail'][0] == "gassemax@mila.quebec":
                # Maxime Gasse is a postdoc with Andrea Lodi but also appears to co-supervise someone.
                is_prof = False

            # Someone can't be prof AND student, apart with the two above exceptions.
            assert not(is_prof and is_student), (
                f"Person {person['givenName'][0]} {person['sn'][0]} is both a student and a prof."
            )

            # because it's stupid to wait for the LDAP to be updated for that one
            if person['mail'][0] == "xhonneul@mila.quebec":
                person['givenName'][0] = "Sophie"

            if is_prof:
                L_profs.append(Prof(person['givenName'][0], person['sn'][0], person['mail'][0], set(cn_groups)))

            if is_student:
                L = [mapping_group_to_prof[prof_short_name] for prof_short_name in cn_groups_of_supervisors]
                if len(L) == 1:
                    # this is not true for students with a principal supervisor
                    # outside of Mila, but it's the best that we can do with the
                    # data from the Mila LDAP
                    supervisor, co_supervisor = L[0], None
                elif len(L) == 2:
                    supervisor, co_supervisor = L
                else:
                    raise ValueError(f"More than two supervisors for a student: {L}.")
                L_students.append(
                    Student(person['givenName'][0], person['sn'][0], person['mail'][0], set(cn_groups),
                            supervisor=supervisor, co_supervisor=co_supervisor, university=university))

    return L_profs, L_students


    """
            "memberOf": [
                "cn=c.pal-students,ou=Groups,dc=mila,dc=quebec",
                "cn=clusterusers,ou=Groups,dc=mila,dc=quebec",
                "cn=d.nowrouzezahrai-students,ou=Groups,dc=mila,dc=quebec",
                "cn=edi.survey.students,ou=Groups,dc=mila,dc=quebec",
                "cn=mcgill-students,ou=Groups,dc=mila,dc=quebec",
                "cn=mila_acces_special,ou=Groups,dc=mila,dc=quebec",
                "cn=phd,ou=Groups,dc=mila,dc=quebec"
            ],
    """

"""
Last Name,First Name,Status,Full Mila,Left Mila,School,Supervisor,Co-Supervisor,Start Date,End Date
Zumer,Jeremie,MSc,Full,Left,UdeM,Aaron Courville,n/a,2017-03-28,2019-03-30
"""

def update_students_from_excel_source(L_students, L_excel_students):
    """
    Mutates `L_students` but leaves `L_excel_students` unchanged.
    """
    L_names_A = [s.first_name + s.last_name for s in L_students]
    L_names_B = [s["first_name"] + s["last_name"] for s in L_excel_students]
    LP_results = find_exact_bag_of_words_matches(L_names_A, L_names_B, delta_threshold=1)
    # see if that makes sense
    # print(LP_results)

    DP_results = {}  # the name to which it matches, and its distance
    for a, b, delta in LP_results:
        # If we have nothing for that name,
        # or if we have something but found an even better match,
        # then update it.
        if a not in DP_results or delta < DP_results[a][1]:
            DP_results[a] = (b, delta)

    D_name_A_to_student = {s.first_name + s.last_name: s for s in L_students}
    D_name_B_to_excel_student = {s["first_name"] + s["last_name"]: s for s in L_excel_students}

    for name_A, (name_B, delta) in DP_results.items():
        student = D_name_A_to_student[name_A]
        excel_student = D_name_B_to_excel_student[name_B]
        # Now we want to update the student with the information from the excel_student.
        # Basically, all that we care about is the supervisor and the co-supervisor.

        # What's not fun, though, is that we're left with `student` having emails
        # to identity the supervisor+cosupervisor, but the excel_student has the
        # full names, and sometimes this even corresponds to non-mila profs.

        # TODO : If you don't have something here, then this function does nothing at all.


def run(population_mila_csv_input_path, mila_raw_ldap_json_input_path, verbose=True):

    L_profs, L_students = read_mila_raw_ldap_json(mila_raw_ldap_json_input_path)

    L_excel_students = read_population_mila_csv(population_mila_csv_input_path)
    update_students_from_excel_source(L_students, L_excel_students)  # mutates `L_students`

    if verbose:
        print("Profs:")
        for prof in L_profs:
            # print(prof.first_name, prof.last_name, prof.mila_email_username, prof.cn_groups)
            print(f"{prof.first_name}, {prof.last_name}, {prof.mila_email_username}, {prof.ccri}, {prof.def_account}") #, {prof.cn_groups}")

        print("Students:")
        for student in L_students:
        #    print(student.first_name, student.last_name, student.mila_email_username, student.cn_groups,
        #          student.supervisor, student.co_supervisor)
            print(f"{student.first_name}, {student.last_name}, {student.mila_email_username}, {student.supervisor}, {student.co_supervisor}")

    with open("profs_and_students.json", "w") as f_out:
        json.dump(
            {"profs": 
                [{"first_name": prof.first_name,
                  "last_name": prof.last_name,
                  "mila_email_username": prof.mila_email_username,
                  "academic_email": prof.academic_email,
                  "ccri": prof.ccri,
                  "def_account": prof.def_account } for prof in L_profs],
            "students": [{
                "first_name": student.first_name,
                "last_name": student.last_name,
                "mila_email_username": student.mila_email_username,
                "supervisor": student.supervisor,
                "co_supervisor": student.co_supervisor,
                "university": student.university} for student in L_students]
            }, f_out, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    run(population_mila_csv_input_path="Copy Population Mila_2023-03-21_for Guillaume Alain - Students.csv",
        mila_raw_ldap_json_input_path="mila_raw_users.json")