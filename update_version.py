import os
import sys

PARENT_ARTIFACT_IDS = ['etf-parent', 'ecp-ticker-feed']

try:
    from lxml import etree
    print("running with lxml.etree")
except ImportError:
    try:
        # Python 2.5
        import xml.etree.cElementTree as etree
        print("running with cElementTree on Python 2.5+")
    except ImportError:
        try:
            # Python 2.5
            import xml.etree.ElementTree as etree
            print("running with ElementTree on Python 2.5+")
        except ImportError:
            try:
                # normal cElementTree install
                import cElementTree as etree
                print("running with cElementTree")
            except ImportError:
                try:
                    # normal ElementTree install
                    import elementtree.ElementTree as etree
                    print("running with ElementTree")
                except ImportError:
                    print("Failed to import ElementTree from any known place")

POM_XMLNS="{http://maven.apache.org/POM/4.0.0}"

def path(*names):
    return "/".join(map(tag, names))

def tag(name):
    if name.startswith("."):
        return name
    else:
        return POM_XMLNS + name

def each_pom():
    for root, dirs, files in os.walk('.'):
        if 'target' in dirs:
            dirs.remove('target')
        if 'pom.xml' in files:
            yield os.path.join(root, 'pom.xml')

def is_parent(pom_xml_root):
    artifact_id = pom_xml_root.find(path(".", "artifactId"))
    return artifact_id.text in PARENT_ARTIFACT_IDS

def main(new_version):
    for pom in each_pom():
        print "\n"+ pom
        with open(pom, 'r') as pomfile:
            tree = etree.parse(pomfile)
            root = tree.getroot()
            version = root.find(tag('version'))
            if version is not None:
                print "  Version: " + version.text
                if is_parent(root):
                    version.text = new_version
                    print "  Fixed parent version"
                else:
                    print "  Removing extraneous <version>"
                    root.remove(version)
            if not is_parent(root):
                for element in root.iterfind(path(".", "parent", "version")):
                    if element.tag == tag('version'):
                        print "  Old Parent version: " + element.text
                        if element.text == new_version:
                            print "  Already OK"
                        else:
                            element.text = new_version
                            print "  New Parent version: " + element.text
        xml = etree.tostring(root, xml_declaration=True, pretty_print=True)
        with open(pom, 'w') as pomfile:
            pomfile.write(xml)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print "USAGE: python {} NEW_VERSION".format(sys.argv[0])
    else:
        main(sys.argv[1])
