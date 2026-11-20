"""Phase 6 : surveillance en temps réel.

    replay (thread)  ->  moteur en ligne  ->  processus écrivain SQLite
                                         ->  serveur TCP multi-clients (threads)
                                         ->  tableau de bord Streamlit (lit SQLite)
"""
