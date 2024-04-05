from procset import ProcSet

from oar.lib.globals import get_logger
from oar.lib.hierarchy import find_resource_hierarchies_scattered

logger = get_logger("oar.custom_scheduling")


def path(itvs, hy, reverse=True):
    """
    Find the path leading to a resource within the hierarchy.
    e.g. hy = {'nodes': [ProcSet((0, 7)), ProcSet((8, 15))], 'cpu': [ProcSet((0, 3)), ProcSet((4, 7)), ProcSet((8, 11)), ProcSet((12, 15))], 'core': [ProcSet(0), ProcSet(1), ProcSet(2), ProcSet(3), ProcSet(4), ProcSet(5), ProcSet(6), ProcSet(7), ProcSet(8), ProcSet(9), ProcSet(10), ProcSet(11), ProcSet(12), ProcSet(13), ProcSet(14), ProcSet(15)], 'resource_id': [ProcSet(0), ProcSet(1), ProcSet(2), ProcSet(3), ProcSet(4), ProcSet(5), ProcSet(6), ProcSet(7), ProcSet(8), ProcSet(9), ProcSet(10), ProcSet(11), ProcSet(12), ProcSet(13), ProcSet(14), ProcSet(15)]}
    e.g. itvs = ProcSet((8,11))
    path(itvs, hy) = [ProcSet((8,15)), ProcSet((8,11))]
    """
    acc = []
    for lv in hy:
        for rsc in hy[lv]:
            if itvs.issubset(rsc):
                acc.append(rsc)
                break

    return sorted(acc, key=lambda x: len(x), reverse=reverse)


def compact(itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

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

        # Select unused resources first (top-down).
        hy_levels = map(
            lambda x: sorted(
                x,
                key=lambda i: [
                    len(prev & itvs_cts_slots) for prev in path(i, hy, reverse=reverse)
                ],
                reverse=reverse,
            ),
            hy_levels,
        )
        res = find_resource_hierarchies_scattered(
            itvs_cts_slots, list(hy_levels), hy_nbs
        )
        if res:
            result = result | res
        else:
            return ProcSet()

    return result


def spread(itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=False):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

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
            hy_levels = list(
                map(
                    lambda x: sorted(
                        x,
                        key=lambda i: [
                            len(prev & itvs_cts_slots2) for prev in path(i, hy)
                        ],
                        reverse=not reverse,
                    ),
                    hy_levels,
                )
            )
        except Exception as e:
            logger.info(e)

        res = find_resource_hierarchies_scattered(itvs_cts_slots, hy_levels, hy_nbs)

        if res:
            result = result | res
        else:
            return ProcSet()

    return result


def co_loc(itvs_slots, hy_res_rqts, hy, beginning_slotset):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    return spread(itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True)


def no_pref(itvs_slots, hy_res_rqts, hy, beginning_slotset):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    return compact(itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=False)
