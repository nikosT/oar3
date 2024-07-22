import itertools

from procset import ProcSet

from oar.lib.globals import get_logger
from oar.lib.hierarchy import find_resource_hierarchies_scattered

logger = get_logger("oar.custom_scheduling")


def compact(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    logger.info(session)

    result = ProcSet()
    for hy_res_rqt in hy_res_rqts:
        (hy_level_nbs, constraints) = hy_res_rqt
        hy_levels = []
        hy_nbs = []
        for hy_l_n in hy_level_nbs:
            (l_name, n) = hy_l_n
            hy_levels.append(hy[l_name])
            hy_nbs.append(n)

        itvs_cts_slots = constraints & itvs_slots

        # create a nodes Procset list sorted by min/max free cores
        hy_nodes = sorted(
            hy["network_address"],
            key=lambda i: len(i & itvs_cts_slots),
            reverse=reverse,
        )

        hy_levels = []
        for node in hy_nodes:
            # collect cpu Procset for particular node
            n_cpus = list(filter(lambda p: p.issubset(node), hy["cpu"]))
            # sort cpu Procset list sorted by min/max free cores
            n_cpus = sorted(
                n_cpus, key=lambda i: len(i & itvs_cts_slots), reverse=reverse
            )
            # map cpu Procset to core procset
            hy_levels += list(
                map(ProcSet, itertools.chain.from_iterable(map(iter, n_cpus)))
            )

        # there is an Admission Rule that blocks other resources than core
        # so only 1 resource type will be given
        hy_levels = [hy_levels]

        res = find_resource_hierarchies_scattered(
            itvs_cts_slots, list(hy_levels), hy_nbs
        )
        if res:
            result = result | res
        else:
            return ProcSet()

    return result


def spread(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=False):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    result = ProcSet()
    for hy_res_rqt in hy_res_rqts:
        (hy_level_nbs, constraints) = hy_res_rqt
        hy_levels = []
        hy_nbs = []
        for hy_l_n in hy_level_nbs:
            (l_name, n) = hy_l_n
            hy_levels.append(hy[l_name])
            hy_nbs.append(n)

        itvs_cts_slots = constraints & itvs_slots

        itvs_cts_slots2 = itvs_cts_slots.copy()

        for soc in hy["cpu"]:
            avail_cores = soc & itvs_cts_slots
            itvs_cts_slots -= ProcSet(*avail_cores[int(len(soc) / 2) : len(soc)])

        # Select unused resources first (top-down).
        try:
            # create a nodes Procset list sorted by min/max free cores
            hy_nodes = sorted(
                hy["network_address"],
                key=lambda i: len(i & itvs_cts_slots2),
                reverse=reverse,
            )

            hy_levels = []
            for node in hy_nodes:
                # collect cpu Procset for particular node
                n_cpus = list(filter(lambda p: p.issubset(node), hy["cpu"]))
                # sort cpu Procset list sorted by min/max free cores
                n_cpus = sorted(
                    n_cpus, key=lambda i: len(i & itvs_cts_slots2), reverse=reverse
                )
                # map cpu Procset to core procset
                hy_levels += list(
                    map(ProcSet, itertools.chain.from_iterable(map(iter, n_cpus)))
                )

            # there is an Admission Rule that blocks other resources than core
            # so only 1 resource type will be given
            hy_levels = [hy_levels]

        except Exception as e:
            logger.info(e)

        res = find_resource_hierarchies_scattered(itvs_cts_slots, hy_levels, hy_nbs)

        if res:
            result = result | res
        else:
            return ProcSet()

    return result


def co_loc(session, itvs_slots, hy_res_rqts, hy, beginning_slotset):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    return spread(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True)


def no_pref(session, itvs_slots, hy_res_rqts, hy, beginning_slotset):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    return compact(itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=False)


def f_compact(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    avail_procset = compact(
        itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=reverse
    )

    # if no allocation space is found (by compact policy)
    # fallback to spread policy
    if len(avail_procset) == 0:
        return spread(itvs_slots, hy_res_rqts, hy, beginning_slotset)
    else:
        return avail_procset


def f_spread(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=False):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    avail_procset = spread(
        itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=reverse
    )

    # if no allocation space is found (by compact policy)
    # fallback to spread
    if len(avail_procset) == 0:
        return compact(itvs_slots, hy_res_rqts, hy, beginning_slotset)
    else:
        return avail_procset


def f_co_loc(session, itvs_slots, hy_res_rqts, hy, beginning_slotset):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    return f_spread(itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True)
