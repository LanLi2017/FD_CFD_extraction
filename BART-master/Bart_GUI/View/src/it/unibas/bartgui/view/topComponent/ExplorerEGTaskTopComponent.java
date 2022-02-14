/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unibas.bartgui.view.topComponent;

import it.unibas.bartgui.view.ViewResource;
import it.unibas.centrallookup.CentralLookup;
import java.awt.BorderLayout;
import java.awt.Color;
import javax.swing.Action;
import javax.swing.ActionMap;
import javax.swing.JScrollPane;
import org.netbeans.api.settings.ConvertAsProperties;
import org.openide.awt.ActionID;
import org.openide.awt.ActionReference;
import org.openide.explorer.ExplorerManager;
import org.openide.explorer.ExplorerUtils;
import org.openide.explorer.view.BeanTreeView;
import org.openide.loaders.DataObject;
import org.openide.nodes.Node;
import org.openide.util.Lookup;
import org.openide.util.LookupEvent;
import org.openide.util.LookupListener;
import org.openide.windows.TopComponent;
import org.openide.util.NbBundle.Messages;

/**
 * Top component which displays something.
 */
@ConvertAsProperties(
        dtd = "-//it.unibas.bartgui.view.topComponent//ExplorerEGTask//EN",
        autostore = false
)
@TopComponent.Description(
        preferredID = "ExplorerEGTaskTopComponent",
        iconBase = "it/unibas/bartgui/resources/icons/gear.png",
        persistenceType = TopComponent.PERSISTENCE_ALWAYS
)
@TopComponent.Registration(mode = "explorer", openAtStartup = true)
@ActionID(category = "Window", id = "it.unibas.bartgui.view.topComponent.ExplorerEGTaskTopComponent")
@ActionReference(path = "Menu/Window" , position = 10 )
@TopComponent.OpenActionRegistration(
        displayName = "#CTL_ExplorerEGTaskAction",
        preferredID = ViewResource.TOP_ID_ExplorerEGTaskTopComponent
)
@Messages({
    "CTL_ExplorerEGTaskAction=EGTask Settings Explorer",
    "CTL_ExplorerEGTaskTopComponent=Explorer EGTask",
    "HINT_ExplorerEGTaskTopComponent=EGTask configuration settings.."
})
public final class ExplorerEGTaskTopComponent extends TopComponent implements ExplorerManager.Provider{

    private transient ExplorerManager ex = new ExplorerManager();
    private Lookup.Result<DataObject> resultDO;
    private LookupDataListener listener = new LookupDataListener();
    private JScrollPane btvw;
    
    public ExplorerEGTaskTopComponent() {
        initComponents();
        setDisplayName(Bundle.CTL_ExplorerEGTaskTopComponent());
        setName(ViewResource.TOP_NAME_ExplorerEGTaskTopComponent);
        setToolTipText(Bundle.HINT_ExplorerEGTaskTopComponent());
        setBackground(Color.WHITE);
        setLayout(new BorderLayout());
        btvw = new BeanTreeView();
        add(btvw, BorderLayout.CENTER);
        ActionMap  map = this.getActionMap();
        //map.put(DefaultEditorKit.copyAction, ExplorerUtils.actionCopy(m));
        //map.put(DefaultEditorKit.cutAction, ExplorerUtils.actionCut(m));
        //map.put(DefaultEditorKit.pasteAction, ExplorerUtils.actionPaste(m));
        map.put("delete", ExplorerUtils.actionDelete(ex, true));
        setRootContext(new SimpleBaseNode());
        associateLookup(ExplorerUtils.createLookup(ex, getActionMap()));
    }

    @Override
    public ExplorerManager getExplorerManager() {
        return ex;
    }
    
    public void setRootContext(Node root)   {
        ex.setRootContext(root);
    }


    @Override
    public Action[] getActions() {
        return super.getActions();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 400, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 300, Short.MAX_VALUE)
        );
    }// </editor-fold>//GEN-END:initComponents

    // Variables declaration - do not modify//GEN-BEGIN:variables
    // End of variables declaration//GEN-END:variables
    @Override
    public void componentOpened() {
        resultDO = CentralLookup.getDefLookup().lookupResult(DataObject.class);
        resultDO.addLookupListener(listener);
    }

    @Override
    public void componentClosed() {
        resultDO.removeLookupListener(listener);
    }

    void writeProperties(java.util.Properties p) {
        // better to version settings since initial version as advocated at
        // http://wiki.apidesign.org/wiki/PropertyFiles
        p.setProperty("version", "1.0");
        // TODO store your settings
    }

    void readProperties(java.util.Properties p) {
        String version = p.getProperty("version");
        // TODO read your settings according to their version
    }
    
    private class LookupDataListener implements LookupListener   {
        @Override
        public void resultChanged(LookupEvent ev) {
            DataObject egtDo = CentralLookup.getDefLookup().lookup(DataObject.class);
            if(egtDo!=null){
                ExplorerEGTaskTopComponent.this.setRootContext(egtDo.getNodeDelegate());
            }else{
                ExplorerEGTaskTopComponent.this.setRootContext(new SimpleBaseNode());
            }
        }      
    }
}
